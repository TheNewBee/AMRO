# AMN AMRO transaction summary

Current **daily net-position** for System A future trades: each complete `Input.txt` record is captured as a Kafka event, aggregated with Kafka Streams, and served as one sorted snapshot.

This is not a blotter of every line. The 717 supplied records collapse to **five** client/product rows. JSON, CSV download, and `Output.csv` are the same snapshot — the UI is only a client of that snapshot.

## Quick start

One command. It checks Java 21+, Maven, Node 20+/npm, and Docker, asks before installing or building anything missing, then starts Kafka, backend, and frontend.

```bash
./scripts/up.sh
```

| | |
| --- | --- |
| UI | [http://localhost:8081](http://localhost:8081) |
| JSON | [http://localhost:8080/api/summary](http://localhost:8080/api/summary) |
| CSV | [http://localhost:8080/api/summary.csv](http://localhost:8080/api/summary.csv) |

```bash
./scripts/up.sh -d          # detached
./scripts/up.sh -y          # accept install/build prompts
./scripts/up.sh --rebuild   # rebuild jar + SPA even if they exist
```

Empty `/data` is seeded with the checked-in 717-record `Input.txt`. After the table appears, you should see the five rows in [`sample/Output.csv`](sample/Output.csv). Stop with Ctrl+C, or `docker compose down`.

Walkthrough with a live screenshot: [`demo-usage.html`](demo-usage.html). Full spec: [`SPEC.md`](SPEC.md).

## Design

The bottleneck to avoid is **re-deriving the report from the whole file on every read**. Capture, aggregation, and presentation are three stages; only the last is what HTTP and the UI touch.

```
System A appends Input.txt
        │  tail + checkpoint (1s poll)
        ▼
   validate 315 / 176|303
        │
        ├─ malformed ──► Kafka DLQ (position + error)
        └─ valid     ──► transactions topic (key = source offset)
                              │
                              ▼
                    Kafka Streams (exactly_once_v2)
                    group by client|product, sum (long − short)
                    state store + changelog
                              │
                              ▼
                    in-memory snapshot; Output.csv ≤1s atomic flush
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
         Output.csv    GET /api/summary   GET /api/summary.csv
                              │
                              ▼
                    Angular table (5s poll, exhaustMap)
```

**Capture.** Kafka does not read the file. The backend tails `Input.txt`, publishes complete lines, and checkpoints byte offset plus a 4 KB prefix hash. Truncation or rewrite of consumed bytes is a contract violation. Malformed records go to the DLQ; ingestion continues.

**Aggregate.** The same JVM runs Kafka Streams. Duplicate event ids are dropped, net quantity is summed per client/product, and changelogs recover the store after restart. One topic partition, one backend replica: one ordered snapshot, no scatter/gather.

**Present.** HTTP and `Output.csv` read the in-memory snapshot (sorted, canonical signed integers). Angular talks HTTP only; nginx proxies `/api/` to the backend.

## Bottlenecks

| Cost if naive | What this stack does |
| --- | --- |
| Re-parse the daily file on every UI refresh or CSV download | Running totals in a Streams `KTable`; API serves the snapshot |
| Rewrite `Output.csv` on every accepted event (717 writes → 5 rows) | Dirty flag, ≤1s atomic replace — readers never see a partial file |
| Hash the whole consumed prefix on every poll | SHA-256 of the first 4 KB (`O(1)` per poll) |
| `send().get()` inside the read loop | Pipeline the poll’s produces, then wait for acks before checkpoint |
| Unbounded RocksDB RAM in one JVM | Shared 32 MiB block cache and 8 MiB write-buffer cap |
| Nightly batch that misses intra-day appends | 1s tail of complete lines only; startup consumes what is already there |
| One bad line stalling the day | Validate, DLQ, continue |
| Kafka retry / process restart double-counting | Event id = source offset; `exactly_once_v2`; seen-id store |

HTTP stays cheap because it never joins Kafka. The UI polls every 5s with `exhaustMap` so a slow response cannot pile up. Production Kafka should set `__transaction_state` RF ≥ 3; this repo’s broker is RF=1 for a complete local deploy.

## API

```bash
curl http://localhost:8080/api/summary
curl -OJ http://localhost:8080/api/summary.csv
```

JSON rows: `clientInformation`, `productInformation`, `totalTransactionAmount`. CSV: `text/csv`, filename `Output.csv`, header `Client_Information,Product_Information,Total_Transaction_Amount`.

## Contracts

- Records are 176 or 303 characters after line terminators; positions 177–303 are ignored filler (supplied file vs spec length).
- Client key: `CLIENT TYPE\|CLIENT NUMBER\|ACCOUNT NUMBER\|SUBACCOUNT NUMBER`. Product key: `EXCHANGE CODE\|PRODUCT GROUP CODE\|SYMBOL\|EXPIRATION DATE`. Components trimmed.
- Net = signed long − signed short. Blank/`+` positive, `-` negative; exact integer arithmetic, no leading zeroes.
- `Input.txt` is append-only. `Output.csv` is replaced, never appended.
- Default topics: `amn-amro-transactions`, `amn-amro-transactions-dead-letter`. Override `KAFKA_BOOTSTRAP_SERVERS` to point at a company cluster.

## Kubernetes

Images must already be built and loaded. Then:

```bash
kubectl apply -f k8s/
kubectl -n amn-amro get pods,svc,pvc
```

One backend replica, PVC at `/data`, wait-for-kafka init, actuator probes, non-root uid 999. Frontend nginx → `http://amn-amro-backend:8080`. Not in this repo: multi-broker HA, Strimzi, leader election, multi-writer files.

## Verify

```bash
scripts/e2e-stack.sh compose    # or k8s
```

Checks the 717→5 oracle through the deployed Kafka, then appends a live record and a dead-letter line.

## Local loop (optional)

`docker compose up kafka`, backend on `:8080` with writable `Input.txt` / `Output.csv` (defaults `/data/...`), `npm --prefix frontend start` on `:4200` (proxies `/api` → `localhost:8080`).
