# StreamForge: PostgreSQL CDC → Kafka Streams → Elasticsearch + AI Gateway (LiteLLM or Portkey) (Local Windows Lab)

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
                                                                        ▲
Natural-language search → Spring Boot → LiteLLM → OpenRouter ──────────┘
```

## Prerequisites

- Windows with PowerShell
- PostgreSQL installed and running locally
- Docker Desktop installed and running (about 4 GB of memory free for Elasticsearch and Kibana)
- Java 21 (the Gradle wrapper `gradlew` is included)
- An OpenRouter API key (used by LiteLLM for the configured free chat models)
- Access to PostgreSQL as an administrator
- `psql` or pgAdmin

## Quick start (after PostgreSQL is prepared)

Complete steps 1–4 once (logical replication, demo database and tables, Debezium user, publication). Copy `.env.example` to `.env`, add your OpenRouter API key, and set unique random values for `LITELLM_MASTER_KEY` and `LITELLM_DB_PASSWORD`. The LiteLLM master key must start with `sk-`. The database password is embedded in LiteLLM's PostgreSQL URL, so use URL-safe characters (a 32-byte hex value is suitable). Then, from `C:\Workspace\StreamForge`:

1. Start the infrastructure and LiteLLM gateway: `docker compose up -d` (step 5).
2. Put your `debezium_user` password in `connectors\register-connector.json`, then register the connector (step 6).
3. Confirm the connector and its task are `RUNNING` (step 6).
4. Start the Spring Boot app: `.\gradlew bootRun` (step 8).
5. Insert, update or delete rows in `public.customer` / `public.address` and check Elasticsearch or Kibana (step 9); try AI-assisted search (step 9c).

## Project layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | ZooKeeper, Kafka, Kafka Connect (Debezium), Elasticsearch, Kibana, NGINX, LiteLLM |
| `litellm\config.yaml` | OpenRouter-backed `chat-default` and `chat-quality` model aliases |
| `portkey\config.json` | Reference Portkey Config (fallback, retry, timeout, cache); paste into the Portkey portal |
| `connectors\register-connector.json` | Debezium PostgreSQL connector definition |
| `src\main\resources\application.yml` | Kafka, Elasticsearch, LiteLLM gateway and server settings (port `8081`) |
| `src\main\java\...\api\AiCustomerSearchController.java` | Natural-language customer search endpoint |
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
| `nginx` | 8080 | Search API proxy; also exposes only `POST /api/customers/ai-search` |
| `litellm` | 4000 | Local OpenAI-compatible gateway; bound to localhost |
| `litellm-db` | – | Private PostgreSQL database for LiteLLM UI and gateway data |

Set `OPENROUTER_API_KEY` in `.env` to your OpenRouter key and replace `LITELLM_MASTER_KEY` and `LITELLM_DB_PASSWORD` with unique secrets of your own. Generate the master key with `python -c "import secrets; print('sk-' + secrets.token_hex(32))"` and the database password with `python -c "import secrets; print(secrets.token_hex(32))"`. Paste the results into `.env`. The master key protects your local gateway; it is not an OpenRouter credential. Do not commit `.env` or use real customer data in prompts sent to the external model.

LiteLLM Admin UI: open `http://localhost:4000/ui`, sign in as `admin`, and use `LITELLM_MASTER_KEY` as the password unless you configure separate UI credentials. The UI's key management and spend tracking require the `litellm-db` PostgreSQL service, which Compose starts before LiteLLM. If you previously started LiteLLM without a database, set `LITELLM_DB_PASSWORD` and run `docker compose up -d litellm-db litellm`; existing model API requests still use the same gateway URL. The database contents persist in the `litellm-data` Docker volume.

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

## 9b. Search API behind NGINX

NGINX (`nginx\nginx.conf`, container port `8080`) proxies `GET /api/customers/**` plus only `POST /api/customers/ai-search` to the Spring Boot app on the host (`host.docker.internal:8081`). It adds per-IP rate limiting, a 10 s cache for GET requests, timeouts and security headers, and blocks all other paths and methods. Start it with `docker compose up -d nginx` (the app must be running).

```powershell
curl.exe "http://localhost:8080/api/customers/search?q=ann&city=Boston&page=0&size=10"
curl.exe http://localhost:8080/api/customers/1
curl.exe http://localhost:8080/healthz
```

Parameters: `q` (fuzzy text, max 100 chars), `city`, `state`, `page`, `size` (max 50). Responses carry `X-Cache-Status`. A `502` means the Spring Boot app is not running.

## 9c. AI-assisted customer search through an AI gateway (LiteLLM or Portkey)

### Choosing the gateway (single switch)

The app talks to one OpenAI-compatible gateway at a time. Pick it with one setting in `.env`:

```
AI_GATEWAY_PROVIDER=litellm    # default; self-hosted gateway from docker-compose
# AI_GATEWAY_PROVIDER=portkey  # hosted Portkey gateway
```

`application.yml` resolves `streamforge.ai.gateway.*` from `streamforge.ai.providers.<provider>.*`, so no code change is needed to switch. Restart the app after changing it. Spring Boot imports `.env` from the working directory (`optional:file:./.env[.properties]`), so IDE, Gradle and test runs pick it up without Run Configuration variables. Real environment variables still override `.env`. Run from the project root.

| Setting | LiteLLM | Portkey |
|---|---|---|
| Base URL | `LITELLM_BASE_URL` (default `http://localhost:4000`) | `PORTKEY_BASE_URL` (default `https://api.portkey.ai`) |
| Key | `LITELLM_MASTER_KEY` (sent as `Authorization: Bearer`) | `PORTKEY_API_KEY` (sent as `x-portkey-api-key`) |
| Model | `LITELLM_MODEL` (alias, default `chat-default`) | `PORTKEY_MODEL` (e.g. `@<provider-slug>/nvidia/nemotron-3.5-lightning:free`) |
| Routing / fallback | `litellm\config.yaml` | Portkey Config: `portkey\config.json` (reference copy), saved in the portal |
| Provider credentials | `OPENROUTER_API_KEY` in the LiteLLM container | Saved in the Portkey portal (Model Catalog), not in this repo |

Check which gateway served a request in the Spring log. Every AI search log line carries `gateway=<litellm|portkey>`.

### Portkey setup

1. In the Portkey portal open **Model Catalog → Add Provider → OpenRouter**, paste your OpenRouter key and note the slug it creates (for example `@openrouter-integration`).
2. Set in `.env`:
   ```
   AI_GATEWAY_PROVIDER=portkey
   PORTKEY_API_KEY=<your Portkey API key>
   PORTKEY_MODEL=@<provider-slug>/nvidia/nemotron-3.5-lightning:free
   ```
3. Restart the app and send an AI search. The request appears in the Portkey portal under **Logs**.

Optional settings:
- `PORTKEY_CONFIG_ID`: a saved Portkey Config ID (`pc-...`). When set, the app sends `x-portkey-config`. `portkey\config.json` is a reference copy for fallback, retry, timeout and cache; replace the `@REPLACE_WITH_OPENROUTER_PROVIDER_SLUG` placeholders with your slug before saving it in the portal.
- `PORTKEY_PROVIDER` and `PORTKEY_UPSTREAM_API_KEY`: send an inline `x-portkey-provider` with a raw provider key. Leave empty if your workspace has `block_inline_config` enabled (error `inline_provider_blocked`); use the `@slug/model` form instead.

When running on Portkey, LiteLLM is not called, so its container can be stopped. The LiteLLM sections below (aliases, PII guardrail, UI) apply only when `AI_GATEWAY_PROVIDER=litellm`. Portkey does not apply the `pii-mask` guardrail; configure guardrails in the Portkey portal if you need them.

### How it works (LiteLLM)

```
Client / Postman → Spring Boot (POST /api/customers/ai-search)
  → LiteLLM gateway (:4000, POST /v1/chat/completions, model alias chat-default)
     → pii-mask guardrail (masks email, phone, SSN, card numbers)
     → OpenRouter free model (nvidia/nemotron-3.5-lightning:free)
        [if it fails after 2 retries → fallback alias chat-quality = google/gemma-4-31b-it:free]
  ← JSON filters {q, lastName, city, state}
  → Spring validates the filters and runs a typed Elasticsearch query
  ← JSON response {found, message, filters, results}
```

The model only turns the sentence into filters. It never sees customer data and never builds queries. Spring sends a model **alias** (`chat-default`), and LiteLLM maps it to the real model in `litellm\config.yaml`.

### Model aliases, retries and fallback

| Alias | OpenRouter model | Role |
|---|---|---|
| `chat-default` | `nvidia/nemotron-3.5-lightning:free` | Used by Spring (`streamforge.ai.gateway.model`) |
| `chat-quality` | `google/gemma-4-31b-it:free` | Fallback when `chat-default` fails |

`router_settings` in `litellm\config.yaml` sets `num_retries: 2` and `chat-default → chat-quality`. Set `LITELLM_MODEL=chat-quality` to call the second alias directly. Models added in the LiteLLM UI are stored in the `litellm-db` volume and picked up automatically, but they are not in git; keep the reproducible setup in `config.yaml`.

### PII guardrail

`litellm\config.yaml` defines the built-in `pii-mask` guardrail (`litellm_content_filter`, `pre_call`, `default_on: true`). It masks email, US phone, SSN and Visa card numbers in prompts before they reach OpenRouter. Last names are intentionally not masked because they are the search filter. Test it:

```powershell
$k = (Get-Content .env | Where-Object { $_ -match '^LITELLM_MASTER_KEY=' }) -replace '^LITELLM_MASTER_KEY=', ''
$b = @{ guardrail_name = 'pii-mask'; text = 'Find john@example.com phone 919-555-1234 last name Smith' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:4000/guardrails/apply_guardrail -Headers @{ Authorization = "Bearer $k" } -ContentType application/json -Body $b
```

Expected: `Find [EMAIL_REDACTED] phone [US_PHONE_REDACTED] last name Smith`. In the UI Playground, select `pii-mask`. Guardrails created only in the UI with the Presidio provider will not load unless Presidio analyzer/anonymizer containers are running. To switch to Presidio later, add those two containers to `docker-compose.yml`, set `PRESIDIO_ANALYZER_API_BASE` and `PRESIDIO_ANONYMIZER_API_BASE` on the `litellm` service, and use `guardrail: presidio` with `pii_entities_config` (omit `PERSON`, or last-name search breaks). No Spring changes are needed.

### LiteLLM UI map

Open `http://localhost:4000/ui`. Logs and Usage are under Observability; Models + Endpoints (including Model Retry Settings and Health Status), Virtual Keys, Playground and Guardrails are under AI Gateway; Teams and Internal Users are under Access Control. Use Logs to confirm which real model answered and whether a fallback happened.

### Run it step by step

1. `.env`: set `OPENROUTER_API_KEY`, a unique `LITELLM_MASTER_KEY` (starts with `sk-`) and a separate `LITELLM_DB_PASSWORD`.
2. `docker compose up -d`, register the Debezium connector (step 6), and make sure the index contains a customer with last name Smith.
3. Provide the settings to Spring. The app imports `.env` from the project root automatically, so no Run Configuration variables are needed. Stop any process already on port 8081.
4. Start the app and send the request (Postman or PowerShell).
5. Watch the Spring log: `AI search request received (gateway=...)`, `Sending AI search request to gateway=... at ... using model ...`, `Gateway ... returned an AI search response in N ms`, `AI search intent parsed`, `AI customer search completed with N matching results`, `AI search request finished: gateway=...`. Prompts, keys and filter values are never logged.

### Postman

Import `postman\StreamForge-AI-Search.postman_collection.json` (variable `baseUrl` = `http://localhost:8081`). It includes last name Smith, city/state, no-match and invalid-prompt requests. Raise the Postman timeout to at least 60 s; free models can take 10 s or more.

Request body: `{"prompt": "Find customers with last name Smith", "page": 0, "size": 10}`.

Responses are always JSON:

```json
{ "found": true, "message": null,
  "filters": { "q": null, "lastName": "Smith", "city": null, "state": null },
  "results": { "total": 1, "page": 0, "size": 10, "items": [ { "customerId": 1, "firstName": "Jane", "lastName": "Smith" } ] } }
```

No match returns HTTP 200 with `"found": false`, `"message": "No customers found matching your search."` and `"results": { "total": 0, "items": [] }`. Errors return `{"error": "..."}`: `400` for an invalid body or prompt (1 to 500 characters), `502` when LiteLLM, OpenRouter or Elasticsearch is unavailable or the model output is unusable.

The app runs on the Windows host and imports `.env` from the project root on startup. If you prefer, you can also export the variables in the PowerShell session; real environment variables override `.env`:

```powershell
$env:LITELLM_MASTER_KEY = (Get-Content .env | Where-Object { $_ -match '^LITELLM_MASTER_KEY=' }) -replace '^LITELLM_MASTER_KEY=', ''
# Optional: use the other configured LiteLLM alias.
# $env:LITELLM_MODEL = "chat-quality"
.\gradlew bootRun
```

Once the Debezium snapshot has populated the `customer-search` index, call the endpoint directly:

```powershell
$body = @{ prompt = "Find customers in Austin, Texas"; page = 0; size = 10 } |
    ConvertTo-Json
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8081/api/customers/ai-search" `
    -ContentType "application/json" `
    -Body $body
```

Or use the NGINX route at `http://localhost:8080/api/customers/ai-search`. NGINX allows POST only on that exact path and does not cache AI requests. The gateway defaults to `chat-default`; set `LITELLM_MODEL=chat-quality` before starting Spring Boot to use the other configured alias. Both currently route to free OpenRouter models, whose availability and rate limits can change. Check `docker compose logs --tail 100 litellm` and the Spring Boot log when the endpoint returns `502`.

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
| AI search returns `502` / "<gateway> API key is not configured" | The key for the selected gateway is empty (`LITELLM_MASTER_KEY` or `PORTKEY_API_KEY`). Check `.env` is in the project root (the working directory) and restart. Then check `docker compose logs litellm` for LiteLLM. |
| Log still says `gateway=litellm` after setting Portkey | `AI_GATEWAY_PROVIDER=portkey` was not read. Confirm it is in `.env` in the working directory, or set it as an environment variable, then restart. |
| Portkey `Following keys are not valid: <slug>` | No provider with that slug exists in Model Catalog. Copy the exact slug from the portal into `PORTKEY_MODEL` (`@slug/model`). |
| Portkey `Either x-portkey-config or x-portkey-provider header is required` | The model has no `@slug/` prefix and no `PORTKEY_CONFIG_ID` is set. Use `@slug/model` in `PORTKEY_MODEL`. |
| Portkey `inline_provider_blocked` | The workspace blocks inline providers. Leave `PORTKEY_PROVIDER` empty and use a saved provider via `@slug/model`. |
| LiteLLM UI: "Not connected to DB!" | Start `litellm-db` (needs `LITELLM_DB_PASSWORD`) and `docker compose up -d litellm`. |
| UI Playground: "Guardrail ... not found" | A UI-created Presidio guardrail cannot load without Presidio containers. Use `pii-mask` from `config.yaml` and delete the broken one. |
| AI search is slow or times out | Free OpenRouter models can take 10 s or more or be rate limited. Raise the client timeout and check LiteLLM Logs for fallbacks. |
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
