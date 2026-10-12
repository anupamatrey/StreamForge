# PostgreSQL CDC with Debezium and Kafka (Local Windows Lab)

This guide sets up a local Change Data Capture (CDC) pipeline using:

- PostgreSQL running directly on Windows
- Docker Desktop
- Apache Kafka
- Kafka Connect with the Debezium PostgreSQL connector
- A sample `customers` table

The pipeline captures committed `INSERT`, `UPDATE`, and `DELETE` changes from PostgreSQL and publishes them to a Kafka topic.

> **Scope:** This is a local learning environment, not a production deployment. The example uses demo credentials and a simple Kafka/ZooKeeper setup. Do not reuse the example password or expose these services publicly.

## Architecture

```text
PostgreSQL (Windows)
  └─ WAL / logical replication (pgoutput)
       └─ Debezium PostgreSQL Connector (Kafka Connect in Docker)
            └─ Kafka topic: localserver.public.customers
                 └─ Spring Boot consumer (CDCConsumer) → aggregation service
```

## Prerequisites

- Windows with PowerShell
- PostgreSQL installed and running locally
- Docker Desktop installed and running
- Java 21 (the Gradle wrapper `gradlew` is included)
- Access to PostgreSQL as an administrator
- `psql` or pgAdmin

## Quick start (after PostgreSQL is prepared)

Complete steps 1–4 once (PostgreSQL logical replication, demo database, Debezium user, publication). Then, from `C:\Workspace\StreamForge`:

1. Start Kafka, ZooKeeper and Kafka Connect: `docker compose up -d` (step 5).
2. Put your `debezium_user` password in `connectors\register-connector.json`, then register the connector (step 6).
3. Confirm the connector and its task are `RUNNING` (step 6).
4. Start the Spring Boot consumer: `.\gradlew bootRun` (step 8).
5. Insert, update or delete a row in `public.customers` and watch the app print the event (step 9).

## Project layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | ZooKeeper, Kafka (KRaft listeners) and Kafka Connect with Debezium |
| `connectors\register-connector.json` | Debezium PostgreSQL connector definition |
| `src\main\resources\application.yml` | Spring Boot Kafka settings (`localhost:9092`, port `8081`) |
| `src\main\java\...\config\KafkaConfig.java` | Kafka consumer factory and listener container factory |
| `src\main\java\...\consumer\CDCConsumer.java` | Listens on `localserver.public.customers` and extracts `op`, `before`, `after` |

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

Check whether PostgreSQL is accepting connections (if the PostgreSQL command-line utilities are available):

```powershell
pg_isready
```

You can also test a connection:

```powershell
psql -h localhost -p 5432 -U postgres -d postgres
```

Use the password configured during installation.

## 2. Enable PostgreSQL logical replication

Connect as a PostgreSQL administrator using pgAdmin or `psql` and check the current settings:

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

Restart the PostgreSQL Windows service. First identify the exact service name:

```powershell
Get-Service *postgres*
```

Then, from PowerShell running as Administrator, replace the example service name with the one shown on your machine:

```powershell
Restart-Service postgresql-x64-17
```

Verify the setting after restart:

```sql
SHOW wal_level;
```

Expected value:

```text
logical
```

> If Docker cannot connect to PostgreSQL, check PostgreSQL's `listen_addresses`, `pg_hba.conf`, Windows Firewall, and the port in use. Allow only the appropriate local Docker network; do not expose PostgreSQL broadly.

## 3. Create a demo database and table

As a PostgreSQL administrator, create the database:

```sql
CREATE DATABASE cdc_demo;
```

Connect to `cdc_demo`, then create the sample table and seed data:

```sql
CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO public.customers (name, email)
VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');
```

## 4. Create a Debezium database user and publication

Run the following in `cdc_demo` as an administrator. Replace the example password with a strong local password.

```sql
CREATE ROLE debezium_user
    WITH LOGIN REPLICATION PASSWORD 'ChangeMe123!';

GRANT CONNECT ON DATABASE cdc_demo TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE public.customers TO debezium_user;
```

Create a publication for the table. Run this as the table owner or a sufficiently privileged database administrator:

```sql
CREATE PUBLICATION dbz_publication
FOR TABLE public.customers;
```

The publication identifies which table changes PostgreSQL makes available to the connector.

## 5. Start Kafka and Debezium with Docker Compose

The `docker-compose.yml` is in the project root. From the project folder:

```powershell
cd C:\Workspace\StreamForge
```

Its content is shown below. Kafka has two listeners: `kafka:29092` for containers (Kafka Connect) and `localhost:9092` for apps running on your machine (the Spring Boot app).

```yaml
services:
  zookeeper:
    image: quay.io/debezium/zookeeper:3.2
    ports:
      - "2181:2181"

  kafka:
    image: quay.io/debezium/kafka:3.2
    depends_on:
      - zookeeper
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    ports:
      - "9092:9092"

  connect:
    image: quay.io/debezium/connect:3.2
    depends_on:
      - kafka
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: "1"
      CONFIG_STORAGE_TOPIC: cdc_connect_configs
      OFFSET_STORAGE_TOPIC: cdc_connect_offsets
      STATUS_STORAGE_TOPIC: cdc_connect_statuses
    ports:
      - "8083:8083"
```

Start the services:

```powershell
docker compose up -d
docker compose ps
```

Wait for Kafka Connect to finish starting, then test its REST API:

```powershell
Invoke-RestMethod http://localhost:8083/
```

If a container does not start, inspect the logs:

```powershell
docker compose logs --tail 100 connect
docker compose logs --tail 100 kafka
```

> **Networking note:** `host.docker.internal` is used below so the connector container can reach PostgreSQL running on the Windows host. Depending on your PostgreSQL and Docker networking configuration, you may need to adjust PostgreSQL's listener and client authentication rules.

## 6. Register the Debezium PostgreSQL connector

The connector definition is in `connectors\register-connector.json` in this project:

```json
{
  "name": "postgres-customers-connector",
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
    "slot.name": "debezium_cdc_demo",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "table.include.list": "public.customers",
    "snapshot.mode": "initial"
  }
}
```

Replace `REPLACE_WITH_YOUR_PASSWORD` with the password used when creating `debezium_user`. Do not commit the real password to source control.

Register the connector from PowerShell, in the project folder:

```powershell
$config = Get-Content .\connectors\register-connector.json -Raw

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body $config
```

Check the connector and task status:

```powershell
Invoke-RestMethod `
  http://localhost:8083/connectors/postgres-customers-connector/status
```

Look for `"state": "RUNNING"` for the connector and its task. If registration fails, read the REST API error and Kafka Connect logs.

> **Re-register after recreating containers.** The Compose file has no volumes, so `docker compose down` or `--force-recreate` deletes the connector. Run the registration command again afterwards. If the connector already exists, `Invoke-RestMethod -Method Delete http://localhost:8083/connectors/postgres-customers-connector` removes it first.

## 7. Consume CDC events from Kafka

Open a second PowerShell window, navigate to the project folder, and start a console consumer:

```powershell
cd C:\Workspace\StreamForge

docker compose exec kafka /kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic localserver.public.customers `
  --from-beginning
```

Leave the consumer running. It prints existing snapshot events and subsequent changes.

## 8. Run the Spring Boot consumer

The app runs on your machine, so it connects to Kafka at `localhost:9092` (set in `application.yml` and `KafkaConfig.java`). Port `8081` must be free.

```powershell
cd C:\Workspace\StreamForge
.\gradlew bootRun
```

On startup, look for `partitions assigned: [localserver.public.customers-0]` in the log. The consumer then prints the extracted payload of each Debezium event:

```text
Received CDC payload: op=c
before=null
after={"id":4,"name":"Dave","email":"dave@example.com",...}
```

- `op` is `c` (create), `u` (update), `d` (delete) or `r` (snapshot read).
- `before` is the row before the change (null for inserts).
- `after` is the row after the change (null for deletes).

Hand these values to your aggregation service from the end of `CDCConsumer.consume()`. Null messages (Debezium tombstones after deletes) and non-JSON messages are skipped.

The consumer group is `streamforge-aggregator` with `auto-offset-reset: earliest`, so on first start it reads the whole topic, including the snapshot rows. Offsets are then committed, so a restart only reads new events. To replay from the start, stop the app, wait about a minute for the group to become inactive, then run:

```powershell
docker compose exec kafka /kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server kafka:29092 `
  --group streamforge-aggregator `
  --topic localserver.public.customers `
  --reset-offsets --to-earliest --execute
```

## 9. Test INSERT, UPDATE, and DELETE

In pgAdmin or `psql`, connect to `cdc_demo`.

### Insert

```sql
INSERT INTO public.customers (name, email)
VALUES ('Charlie', 'charlie@example.com');
```

The consumer window (step 7) and the Spring Boot app (step 8) should show a change event. In the default Debezium envelope, the operation code is `c` (create).

### Update

```sql
UPDATE public.customers
SET email = 'charlie.updated@example.com',
    updated_at = CURRENT_TIMESTAMP
WHERE name = 'Charlie';
```

The event should include the before/after row values, with operation code `u` (update).

### Delete

```sql
DELETE FROM public.customers
WHERE name = 'Charlie';
```

The delete event uses operation code `d`. Depending on converter and connector settings, a subsequent Kafka tombstone record with a null value may also be emitted.

The expected topic name is:

```text
localserver.public.customers
```

The topic name is formed from the configured topic prefix and the PostgreSQL schema/table name.

## 10. Troubleshooting

| Symptom | Checks |
|---|---|
| `psql` is not recognized | PostgreSQL may be installed but its `bin` directory is not on `PATH`. |
| PostgreSQL connection refused | Confirm the service is running, port `5432` is correct, and PostgreSQL is listening on a reachable interface. |
| Password authentication failed | Verify the role/password and the applicable `pg_hba.conf` rule. |
| `wal_level` is not `logical` | Set it with `ALTER SYSTEM`, restart PostgreSQL, and verify again. |
| Replication permission error | Verify `debezium_user` has `LOGIN` and `REPLICATION` privileges. |
| Publication missing | Confirm `dbz_publication` exists in `cdc_demo` and includes `public.customers`. |
| Connector fails to start | Check connector status and `docker compose logs --tail 100 connect`. |
| Connector runs but no topic/events appear | Verify the topic name, table filter, initial snapshot, Kafka logs, and connector task status. |
| Docker cannot reach PostgreSQL | Check `host.docker.internal`, `listen_addresses`, `pg_hba.conf`, Windows Firewall, and the port. |
| PostgreSQL disk usage grows while CDC is stopped | Inspect replication slots and retained WAL. A stalled slot can prevent WAL cleanup. |
| `kafka` service is not running | Run `docker compose logs --tail 100 kafka`. Check that the listener settings in `docker-compose.yml` are unchanged. |
| Kafka Connect stops with "Failed to connect to and describe Kafka cluster" | Connect must use `BOOTSTRAP_SERVERS: kafka:29092`. `localhost:9092` points back at the Connect container itself. |
| `port is already allocated` on 2181, 9092 or 8083 | Another Compose project (for example an old `cdc-labs` stack) is using the port. Stop it with `docker compose down` in that folder. |
| Connector list is empty after a restart | The containers have no volumes, so the connector was lost. Register it again (step 6). |
| Spring Boot says port 8081 is already in use | Stop the other instance, or change `server.port` in `application.yml`. |
| Spring Boot receives nothing | Confirm the connector is `RUNNING`, the topic exists (`kafka-topics.sh --list`), and the app log shows the partition assigned. If the group already committed offsets, reset them (step 8). |

Useful commands:

```powershell
docker compose ps
docker compose logs --tail 100 connect
docker compose logs --tail 100 kafka
Invoke-RestMethod http://localhost:8083/connectors/postgres-customers-connector/status
```

## 11. Important concepts and next steps

- **WAL:** PostgreSQL's Write-Ahead Log records changes that logical decoding can expose.
- **Logical replication:** Lets a consumer read logical row changes rather than raw physical WAL records.
- **Replication slot:** Tracks the connector's progress and can retain WAL while the consumer is behind or offline.
- **Publication:** Specifies the tables whose changes are available through PostgreSQL logical replication.
- **Debezium event envelope:** Usually contains metadata such as operation type and before/after row data.
- **Initial snapshot:** With `snapshot.mode=initial`, Debezium captures existing table rows before streaming ongoing changes.

Possible next steps:

1. Pass `op`, `before` and `after` from `CDCConsumer` to your aggregation service.
2. Add more tables using `table.include.list` and the relevant PostgreSQL publication.
3. Configure JSON converters and schemas explicitly for downstream consumers.
4. Add monitoring for connector/task health, consumer lag, replication slot state, and disk usage.
5. Move credentials into a secure secrets mechanism and use a hardened, supported deployment configuration before production.

## References

- [Debezium PostgreSQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium Docker documentation](https://debezium.io/documentation/reference/stable/operations/docker.html)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL logical replication documentation](https://www.postgresql.org/docs/current/logical-replication.html)
