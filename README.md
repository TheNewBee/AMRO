# AMN AMRO transaction summary

## System design: capture and present

The specified interface is a **current daily net-position report**, not a dump of every System A line. Capture turns each complete `Input.txt` record into a Kafka event as it arrives. Presentation is always the same sorted snapshot through three contracted surfaces: `Output.csv`, `GET /api/summary` (JSON), and `GET /api/summary.csv`. Angular is a client of that snapshot. Docker Compose and Kubernetes deploy Kafka, the backend, and the frontend together.

```
System A appends Input.txt
        │  FileIngestionService tails + checkpoints
        ▼
   validate 315 / 176|303
        │
        ├─ malformed ──► Kafka DLQ (position + error); skip totals
        └─ valid     ──► Kafka transactions topic (JSON Transaction, key = event id)
                              │
                              ▼
                    Kafka Streams (exactly_once_v2)
                    group by client|product, sum (long − short)
                    state store + changelog
                              │
                              ▼
                    ReportService atomically replaces Output.csv
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
         Output.csv    GET /api/summary   GET /api/summary.csv
                              │
                              ▼
                    Angular table, 5s poll + CSV download
```

### Capture (how Kafka receives data)

Kafka does not read the file. The backend **producer** does.

1. `FileIngestionService` polls the append-only `Input.txt` (startup: existing records; afterwards: complete appended lines only). Offset + SHA-256 fingerprint live in `Input.offset`. Truncation or rewrite is a contract violation.
2. `FixedWidthParser` builds client key, product key, and signed delta. Event id is a hash of source position plus raw bytes, so restart/redelivery does not mint a new trade.
3. `KafkaTemplate.send(...).get()` publishes to the configured transactions topic and waits for the broker ack before advancing the checkpoint. Parse failures go to the dead-letter topic instead; ingestion continues.
4. Topic names and bootstrap servers come from the environment (`TRANSACTION_TOPIC`, `DLQ_TOPIC`, `KAFKA_BOOTSTRAP_SERVERS`). Topics default to **one partition** so one backend replica owns a complete ordered snapshot.

### Stream processing (how Kafka is consumed)

The same JVM is a **Kafka Streams** application (`processing.guarantee=exactly_once_v2`). It consumes the transactions topic, drops duplicate event ids, aggregates into a `KTable`, and on each accepted update rewrites `Output.csv`. Changelog topics recover the store after restart. This is not a nightly batch over the whole file: each appended record updates running totals as soon as Streams accepts it. The supplied 717 records collapse to five report rows because they share five client/product keys — every valid line is processed.

Broker-side `__transaction_state` replication factor is set on the Kafka brokers, not by this app. Production clusters should use at least `3`.

### Present (specified interfaces)

JSON and CSV are generated from the **same** `ReportService` in-memory snapshot (sorted by client, then product; canonical signed integers, no leading zeroes).

| Interface | Contract |
| --- | --- |
| File | `Output.csv` replaced atomically after every accepted aggregate (readers never see a partial write) |
| JSON | `GET /api/summary` → `[{ clientInformation, productInformation, totalTransactionAmount }]` |
| CSV | `GET /api/summary.csv` → `text/csv`, `Content-Disposition: attachment; filename="Output.csv"`, header `Client_Information,Product_Information,Total_Transaction_Amount` |
| UI | Angular standalone app: table, visible loading/error/last-refreshed, `exhaustMap` poll every 5s, download link to the CSV endpoint |

Angular does not subscribe to Kafka. Browsers talk HTTP. Nginx serves the SPA and proxies `/api/` to the backend Service.

Checked-in [`sample/Output.csv`](sample/Output.csv) is the independent 717→5 oracle. It is not runtime state. A walkthrough with a live screenshot is [`demo-usage.html`](demo-usage.html).

### Kubernetes placement

| In this repo’s manifests | Not in this repo |
| --- | --- |
| Kafka Deployment + Service (`apache/kafka:3.8.1`, KRaft single broker, RF=1) | Multi-broker HA / Strimzi operator |
| Backend Deployment **replicas: 1**, PVC at `/data`, wait-for-kafka init, actuator probes, non-root uid 999 | Leader election, multi-writer file sharing |
| Frontend Deployment + Service, nginx → `http://amn-amro-backend:8080` | |
| ConfigMap: file paths, bootstrap `kafka:9092`, topic names | |

One backend replica is required for deterministic file ownership and a single Streams snapshot. Override `KAFKA_BOOTSTRAP_SERVERS` if you point at a company cluster instead of the bundled broker.

## Architecture notes

The Spring Boot backend tails the append-only System A `Input.txt`, validates fixed-width transactions, publishes valid events to Kafka, and uses Kafka Streams to aggregate net quantities by client/product. Malformed records are skipped and published to a configurable dead-letter topic with source position and validation details. Streams state/changelogs plus file checkpoint and deduplication metadata support restart recovery.

## Fixed-width and business rules

- Records with 176 or 303 characters are accepted after line terminators are removed. Positions 177–303 are ignored filler, which resolves the supplied 176-character input versus the file specification's 303-character length.
- Blank lines are ignored, and a final record may omit a trailing newline. Record code, length, numeric fields, and signs are validated.
- Client information is `CLIENT TYPE|CLIENT NUMBER|ACCOUNT NUMBER|SUBACCOUNT NUMBER`; product information is `EXCHANGE CODE|PRODUCT GROUP CODE|SYMBOL|EXPIRATION DATE`. Each component is trimmed before joining.
- Long quantity is reduced by short quantity using exact integer arithmetic. Blank and `+` signs are positive; `-` is negative; other signs are invalid. Amounts are canonical signed base-10 integers without leading zeroes.
- Report rows sort by client components and then product components. `Output.csv` is append-free report output: it is replaced atomically after every accepted update, while `Input.txt` remains append-only. Truncation or rewrite is an operational contract violation. Deterministic source/event identity prevents duplicate aggregation after restart or redelivery.

## REST API

```bash
curl http://localhost:8080/api/summary
curl -OJ http://localhost:8080/api/summary.csv
```

JSON rows contain `clientInformation`, `productInformation`, and `totalTransactionAmount`. CSV uses `text/csv`, attachment filename `Output.csv`, and the exact header `Client_Information,Product_Information,Total_Transaction_Amount`.

## Local prerequisites and run paths

Prerequisites: Java 21+, Maven, Node.js/npm with the Angular CLI, and Docker. The complete stack is Kafka + backend + frontend.

For a local JVM/Angular loop without Compose, start the bundled broker (`docker compose up kafka`) or any Kafka at `localhost:9092`, then run the backend from its Spring Boot Maven project and the frontend with its Angular development server. Configure local paths to the checked-in `Input.txt` and a writable `Output.csv`. The runtime defaults are `/data/Input.txt` and `/data/Output.csv`.

For local Angular development, `npm --prefix frontend start` loads `frontend/proxy.conf.json`; relative `/api` requests are proxied from `localhost:4200` to the backend at `localhost:8080`. Start the backend on port 8080, or change the proxy target for another local endpoint.

The independent expected fixture is [`sample/Output.csv`](sample/Output.csv); it is not runtime state.

## Docker

Build the application artifacts first so the conventional paths exist:

```bash
mvn -f backend/pom.xml package
npm --prefix frontend ci
npm --prefix frontend run build

docker build -f docker/backend/Dockerfile -t amn-amro/backend:latest .
docker build -f docker/frontend/Dockerfile -t amn-amro/frontend:latest .
```

The backend image copies `backend/target/*.jar` and seeds `/data/Input.txt` from the checked-in fixture when the volume is empty. The frontend image copies the Angular browser build from `frontend/dist`; the Dockerfile assumes the first project directory there contains the browser assets.

Bring up Kafka, backend, and frontend together:

```bash
docker compose up --build
```

UI is `http://localhost:8081`. Backend API is `http://localhost:8080`. Compose waits for the broker to be healthy before starting the backend. Empty `/data` is seeded with the 717-record `Input.txt`.

## Kubernetes

Build the two application images, load them into the cluster, then apply every manifest. Kafka is included:

```bash
kubectl apply -f k8s/
kubectl -n amn-amro get pods,svc,pvc
```

That creates namespace `amn-amro`, a KRaft Kafka broker Service at `kafka:9092`, a PVC mounted at `/data`, one backend replica (init container waits until Kafka accepts TCP), frontend/backend Services, ConfigMaps, and Spring Boot actuator probes. The frontend proxy expects Service name `amn-amro-backend` in the same namespace. Default topics are `amn-amro-transactions` and `amn-amro-transactions-dead-letter`. Override `KAFKA_BOOTSTRAP_SERVERS` in the backend ConfigMap only if you replace the bundled broker.
