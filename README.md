# AMN AMRO transaction summary

## Architecture

The Spring Boot backend tails the append-only System A `Input.txt`, validates fixed-width transactions, publishes valid events to external Kafka, and uses Kafka Streams to aggregate net quantities by client/product. It atomically rewrites `Output.csv` under `/data` after accepted aggregate updates. Kafka Streams state/changelogs plus file checkpoint and deduplication metadata support restart recovery. Malformed records are skipped and published to a configurable dead-letter topic with source position and validation details.

The Angular frontend polls `GET /api/summary` every five seconds and offers `GET /api/summary.csv`. Nginx serves the SPA and proxies `/api/` to the backend. Kubernetes runs one backend replica with persistent `/data`; Kafka is not deployed by this repository and must be externally managed.

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

Prerequisites: Java 21+, Maven, Node.js/npm with the Angular CLI, and an externally reachable Kafka cluster. Build the backend and frontend using their project instructions, then run the backend from its Spring Boot Maven project and the frontend with its Angular development server. Configure local paths to the checked-in `Input.txt` and a writable `Output.csv`, and configure Kafka bootstrap/topic properties for the local cluster. The runtime defaults are `/data/Input.txt` and `/data/Output.csv`.

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

The backend image copies `backend/target/*.jar`. The frontend image copies the Angular browser build from `frontend/dist`; the Dockerfile assumes the first project directory there contains the browser assets. Run with a writable `/data` volume and external Kafka, for example:

```bash
docker run --rm -p 8080:8080 -v "$PWD:/data" \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 amn-amro/backend:latest
docker run --rm -p 8081:80 amn-amro/frontend:latest
```

The frontend proxy targets `amn-amro-backend:8080` in Kubernetes. For standalone Docker, provide equivalent DNS or adjust the nginx upstream at image-build/deployment time.

## Kubernetes

Apply the manifests after building/pushing the two images and provisioning external Kafka:

```bash
kubectl apply -f k8s/
kubectl -n amn-amro get pods,svc,pvc
```

The manifests create a persistent volume claim mounted at `/data`, one backend replica, frontend/backend Services, ConfigMap-backed runtime file paths, external Kafka bootstrap/topic configuration, and Spring Boot actuator startup, liveness, and readiness probes. Apply them in the namespace used for the deployment; the frontend proxy expects the backend Service name `amn-amro-backend` in that same namespace. Default Kafka settings in `k8s/configmap.yaml` are bootstrap `kafka.company.internal:9092`, transaction topic `amn-amro-transactions`, and dead-letter topic `amn-amro-transactions-dead-letter`; these are examples for the externally managed cluster and operators must override the ConfigMap values for their Kafka hostname and topic names. No Kafka broker or controller is bundled.
