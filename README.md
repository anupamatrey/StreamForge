# StreamForge: PostgreSQL CDC → Kafka Streams → Elasticsearch (Local Windows Lab)

This guide sets up a local pipeline that captures changes from two PostgreSQL tables (`customer`, `address`), joins them with Kafka Streams into one customer document, and indexes that document in Elasticsearch (viewable in Kibana).

> **Scope:** This is a local learning environment, not a production deployment. The examples use demo credentials (`ChangeMe123!`, `elastic` / `changeme`). Do not reuse them or expose these services publicly.

## Architecture

```text
PostgreSQL (Windows)
  └─ WAL / logical replication (pgoutput)
       └─ Debezium PostgreSQL connector (Kafka Connect, Docker)
            ├─ Kafka topic: localserver.public.customer ─┐
            └─ Kafka topic: localserver.public.address ──┤
                                                         ▼
                              CustomerAggregationTopology (Kafka Streams)
                              customers-by-id, addresses-by-id,
                              addresses-by-customer → left join
                                                         ▼
                              Kafka topic: customer-search-documents
                                                         ▼
                              CustomerDocumentIndexer (@KafkaListener)
                                                         ▼
                              Elasticsearch index: customer-search ──► Kibana
```

## Prerequisites

- Windows with PowerShell
- PostgreSQL installed and running locally
- Docker Desktop installed and running (about 4 GB of memory free for Elasticsearch and Kibana)
- Java 21 (the Gradle wrapper `gradlew` is included)
- Access to PostgreSQL as an administrator
- `psql` or pgAdmin

## Quick start (after PostgreSQL is prepared)

Complete steps 1–4 once (logical replication, demo database and tables, Debezium user, publication). Then, from `C:\Workspace\StreamForge`:

1. Start Kafka, ZooKeeper, Kafka Connect, Elasticsearch and Kibana: `docker compose up -d` (step 5).
2. Put your `debezium_user` password in `connectors\register-connector.json`, then register the connector (step 6).
3. Confirm the connector and its task are `RUNNING` (step 6).
4. Start the Spring Boot app: `.\gradlew bootRun` (step 8).
5. Insert, update or delete rows in `public.customer` / `public.address` and check Elasticsearch or Kibana (step 9).

## Project layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | ZooKeeper, Kafka, Kafka Connect (Debezium), Elasticsearch, Kibana |
| `connectors\register-connector.json` | Debezium PostgreSQL connector definition |
| `src\main\resources\application.yml` | Kafka, Elasticsearch and server settings (port `8081`) |
| `src\main\java\...\config\KafkaConfig.java` | Kafka consumer factory and listener container factory |
| `src\main\java\...\config\KafkaStreamsConfig.java` | Kafka Streams settings (application id `streamforge-streams`) |
| `src\main\java\...\streams\CustomerAggregationTopology.java` | Joins customers and addresses into `customer-search-documents` |
| `src\main\java\...\streams\model\` | `Customer`, `Address`, `AddressCollection`, `CustomerDocument` |
| `src\main\java\...\consumer\CustomerDocumentIndexer.java` | Indexes joined documents into Elasticsearch (null value = delete) |

## 1. Check PostgreSQL installation

Open PowerShell:

```powershell
psql --version
Get-Service *postgres*
```

If `psql` is not recognized, PostgreSQL may still be installed; its `bin` directory may simply not be on `PATH`. Check these folders:

```text
C:\Program Files\PostgreSQL\
C:\Program Files (x86)\PostgreSQL\
```

Check whether PostgreSQL is accepting connections:

```powershell
pg_isready
psql -h localhost -p 5432 -U postgres -d postgres
```

Use the password configured during installation.

## 2. Enable PostgreSQL logical replication

Connect as a PostgreSQL administrator and check the current settings:

```sql
SHOW server_version;
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;
```

If needed, set the logical replication parameters:

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
```

Restart the PostgreSQL Windows service. Find the exact service name, then restart it from PowerShell running as Administrator (replace the example name):

```powershell
Get-Service *postgres*
Restart-Service postgresql-x64-17
```

Verify after restart (expected value: `logical`):

```sql
SHOW wal_level;
```

> If Docker cannot connect to PostgreSQL, check PostgreSQL's `listen_addresses`, `pg_hba.conf`, Windows Firewall, and the port in use. Allow only the appropriate local Docker network; do not expose PostgreSQL broadly.

## 3. Create a demo database and tables

As a PostgreSQL administrator, create the database:

```sql
CREATE DATABASE cdc_demo;
```

Connect to `cdc_demo`, then create the tables and seed data. The topology reads exactly these columns:

```sql
CREATE TABLE public.customer (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(200),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.address (
    address_id  SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES public.customer (customer_id),
    street      VARCHAR(200),
    city        VARCHAR(100),
    state       VARCHAR(50),
    postal_code VARCHAR(20),
    country     VARCHAR(100)
);

INSERT INTO public.customer (first_name, last_name, email)
VALUES ('John', 'Smith', 'john@example.com');

INSERT INTO public.address (customer_id, street, city, state, postal_code, country)
VALUES
    (1, '123 Main Street', 'New York', 'NY', '10001', 'USA'),
    (1, '456 Park Avenue', 'Boston', 'MA', '02101', 'USA');
```

## 4. Create a Debezium database user and publication

Run the following in `cdc_demo` as an administrator. Replace the example password with a strong local password.

```sql
CREATE ROLE debezium_user
    WITH LOGIN REPLICATION PASSWORD 'ChangeMe123!';

GRANT CONNECT ON DATABASE cdc_demo TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE public.customer, public.address TO debezium_user;
```

Create a publication for both tables (as the table owner or an administrator):

```sql
CREATE PUBLICATION dbz_publication
FOR TABLE public.customer, public.address;
```

If the publication already exists, add the tables instead:

```sql
ALTER PUBLICATION dbz_publication ADD TABLE public.customer, public.address;
```

Check it:

```sql
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
```

## 5. Start the infrastructure with Docker Compose

From the project folder:

```powershell
cd C:\Workspace\StreamForge
docker compose up -d
docker compose ps
```

This starts:

| Service | Port | Notes |
|---|---|---|
| `zookeeper` | 2181 | |
| `kafka` | 9092 | `kafka:29092` for containers, `localhost:9092` for apps on your machine |
| `connect` | 8083 | Kafka Connect with the Debezium connectors |
| `elasticsearch` | 9200 | Security on; user `elastic`, password `changeme` |
| `kibana-setup` | – | One-shot container that sets the `kibana_system` password, then exits |
| `kibana` | 5601 | Log in with `elastic` / `changeme` |

Wait until Elasticsearch is `healthy` and Kafka Connect responds (it can take a minute; `curl: (52) Empty reply` means it is still starting):

```powershell
Invoke-RestMethod http://localhost:8083/
curl.exe -u elastic:changeme http://localhost:9200
```

If a container does not start, inspect its logs:

```powershell
docker compose logs --tail 100 connect
docker compose logs --tail 100 kafka
docker compose logs --tail 100 elasticsearch
```

> **Networking note:** `host.docker.internal` is used in the connector so the container can reach PostgreSQL running on the Windows host. You may need to adjust PostgreSQL's `listen_addresses` and `pg_hba.conf`.

## 6. Register the Debezium PostgreSQL connector

The connector definition is `connectors\register-connector.json`:

```json
{
  "name": "postgres-customers-connector-v2",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "host.docker.internal",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "REPLACE_WITH_YOUR_PASSWORD",
    "database.dbname": "cdc_demo",
    "topic.prefix": "localserver",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_cdc_demo_v2",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "table.include.list": "public.customer,public.address",
    "snapshot.mode": "initial"
  }
}
```

Replace `REPLACE_WITH_YOUR_PASSWORD` with the `debezium_user` password. Do not commit the real password to source control.

Register it:

```powershell
$config = Get-Content .\connectors\register-connector.json -Raw

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body $config
```

Check the status. Look for `"state": "RUNNING"` for the connector and its task:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/postgres-customers-connector-v2/status
```

Because `snapshot.mode` is `initial`, Debezium first publishes every existing row, which creates the topics `localserver.public.customer` and `localserver.public.address`. Debezium creates a topic only when the first event for that table arrives.

> **Re-register after recreating containers.** The Compose file has no volumes for Kafka or Connect, so `docker compose down` deletes the connector and topics. Register it again afterwards. To remove an existing connector first:
> ```powershell
> Invoke-RestMethod -Method Delete http://localhost:8083/connectors/postgres-customers-connector-v2
> ```
> If you change `table.include.list` on a connector that already has a saved offset, it will not re-snapshot the existing rows. Use a new connector `name` and `slot.name`, and drop the old slot in PostgreSQL: `SELECT pg_drop_replication_slot('debezium_cdc_demo_v2');`

## 7. Watch the Kafka topics (optional)

List the topics. You should see the two source topics:

```powershell
docker compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

In a second PowerShell window, watch the raw change events:

```powershell
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic localserver.public.customer `
  --from-beginning
```

Once the app is running (step 8), watch the joined documents the same way with `--topic customer-search-documents`.

## 8. Run the Spring Boot application

The app runs on your machine, so it connects to Kafka at `localhost:9092` and Elasticsearch at `http://localhost:9200` (set in `application.yml`). Port `8081` must be free. Both source topics must exist before it starts, or Kafka Streams fails to start. Run the connector first (step 6).

```powershell
cd C:\Workspace\StreamForge
.\gradlew bootRun
```

What runs:

- **`CustomerAggregationTopology`** reads the Debezium events from both topics, keeps the latest customer and address state, groups addresses by customer, left-joins them and writes one document per customer to `customer-search-documents`. Deleted rows become tombstones. `created_at` is converted from epoch microseconds to an ISO-8601 timestamp.
- **`CustomerDocumentIndexer`** consumes `customer-search-documents` (group `streamforge-aggregator`) and indexes each document into the `customer-search` index, with the customer id as the document id. It logs every document it writes. A null value deletes the document.

Run the tests (they use an in-memory topology driver, so no Kafka is needed):

```powershell
.\gradlew test
```

## 9. Test INSERT, UPDATE, and DELETE

In pgAdmin or `psql`, connect to `cdc_demo`.

```sql
-- insert a customer, then an address for it
INSERT INTO public.customer (first_name, last_name, email)
VALUES ('Jane', 'Doe', 'jane@example.com');

INSERT INTO public.address (customer_id, street, city, state, postal_code, country)
VALUES (2, '1 Market Street', 'Chicago', 'IL', '60601', 'USA');

-- update
UPDATE public.address SET city = 'Austin' WHERE street = '1 Market Street';

-- delete the address; the customer document stays, without that address
DELETE FROM public.address WHERE street = '1 Market Street';
```

(Use the real `customer_id` returned for Jane if it is not `2`.)

Check Elasticsearch:

```powershell
curl.exe -u elastic:changeme "http://localhost:9200/customer-search/_search?pretty"
curl.exe -u elastic:changeme "http://localhost:9200/customer-search/_doc/1?pretty"
```

Each document holds the customer fields plus an `addresses` list. A customer appears before any of its addresses exist. Changing an address's `customer_id` moves it to the other customer. Deleting a customer removes its document.

In Kibana (http://localhost:5601, `elastic` / `changeme`): Stack Management → Data Views → create a data view for `customer-search`, then open Discover.

> Elasticsearch fixes field types on the first document. If you change the document shape, delete the index and restart the app:
> ```powershell
> curl.exe -u elastic:changeme -X DELETE http://localhost:9200/customer-search
> ```

## 10. Troubleshooting

| Symptom | Checks |
|---|---|
| `psql` is not recognized | PostgreSQL may be installed but its `bin` directory is not on `PATH`. |
| PostgreSQL connection refused | Confirm the service is running, port `5432` is correct, and PostgreSQL is listening on a reachable interface. |
| Password authentication failed | Verify the role/password and the applicable `pg_hba.conf` rule. |
| `wal_level` is not `logical` | Set it with `ALTER SYSTEM`, restart PostgreSQL, and verify again. |
| Replication permission error | Verify `debezium_user` has `LOGIN` and `REPLICATION` privileges. |
| Publication missing a table | Run the `pg_publication_tables` query (step 4) and `ALTER PUBLICATION ... ADD TABLE`. |
| `curl: (52) Empty reply from server` on 8083 | Kafka Connect is still starting. Wait about 30 seconds and retry. |
| Connector fails to start | Check connector status and `docker compose logs --tail 100 connect`. |
| Topics for `customer` / `address` are missing | Debezium creates a topic when the first event arrives. The connector may have an old saved offset and not snapshotted. Insert or update a row, or register under a new name and slot (step 6). |
| Docker cannot reach PostgreSQL | Check `host.docker.internal`, `listen_addresses`, `pg_hba.conf`, Windows Firewall, and the port. |
| PostgreSQL disk usage grows while CDC is stopped | Inspect replication slots and retained WAL. A stalled slot can prevent WAL cleanup. |
| Kafka Connect "Failed to connect to and describe Kafka cluster" | Connect must use `BOOTSTRAP_SERVERS: kafka:29092`. |
| `port is already allocated` on 2181, 9092, 8083, 9200 or 5601 | Another Compose project is using the port. Run `docker compose down` there. |
| Elasticsearch container exits | Docker Desktop needs enough memory (4 GB or more). Check `docker compose logs elasticsearch`. |
| Kibana does not start | `kibana-setup` must finish first. Check `docker compose logs kibana-setup kibana`. |
| Spring Boot says port 8081 is already in use | Stop the other instance, or change `server.port` in `application.yml`. |
| App fails at startup with missing topics | Kafka Streams needs `localserver.public.customer` and `localserver.public.address` to exist. Register the connector and wait for the snapshot first. |
| Nothing in `customer-search` | Confirm the connector is `RUNNING`, check the `customer-search-documents` topic, and read the app log for `Indexing customer ...`. |
| Elasticsearch authentication errors in the app log | Credentials in `application.yml` must match `ELASTIC_PASSWORD` in `docker-compose.yml`. |

Useful commands:

```powershell
docker compose ps
docker compose logs --tail 100 connect
Invoke-RestMethod http://localhost:8083/connectors/postgres-customers-connector-v2/status
curl.exe -u elastic:changeme "http://localhost:9200/_cat/indices?v"
```

## 11. Important concepts and next steps

- **WAL / logical replication:** PostgreSQL's Write-Ahead Log records changes that logical decoding exposes as row changes.
- **Replication slot:** Tracks the connector's progress and can retain WAL while the consumer is offline.
- **Publication:** Specifies which tables' changes are available through logical replication.
- **Debezium event envelope:** Contains the operation (`op`: `c`, `u`, `d`, `r`) and `before` / `after` row data. The topology reads these from `payload`.
- **KTable:** The latest value per key. Changing an address's `customer_id` moves it between customer collections, and a delete removes the key.
- **Initial snapshot:** With `snapshot.mode=initial`, Debezium captures existing rows before streaming changes.

Possible next steps:

1. Add monitoring for connector health, consumer lag, replication slot state and disk usage.
2. Move credentials into a secrets mechanism and enable TLS before any real deployment.
3. Add more tables by extending the publication, `table.include.list` and the topology.

## References

- [Debezium PostgreSQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Apache Kafka Streams documentation](https://kafka.apache.org/documentation/streams/)
- [Spring for Apache Kafka](https://docs.spring.io/spring-kafka/reference/)
- [Elasticsearch documentation](https://www.elastic.co/docs/)
- [PostgreSQL logical replication documentation](https://www.postgresql.org/docs/current/logical-replication.html)
