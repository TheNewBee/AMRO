## Problem Statement

Business users receive future-transaction records from System A in a fixed-width `Input.txt` file. They need a reliable daily view of net transaction quantity for each unique client/product combination, but no application currently parses, aggregates, presents, or continuously updates that information.

Supplied artifacts conflict on record length: file specification describes fields through position 176 plus filler through position 303, while supplied input contains 717 records of 176 characters. Solution must process supplied data, remain compatible with documented 303-character records, expose current results through API and UI, continuously update `Output.csv`, and demonstrate production-oriented streaming and Kubernetes practices.

## Solution

Build Java 21+ Spring Boot application that reads existing and appended records from append-only `Input.txt`, validates and publishes valid transactions to Kafka, aggregates net quantities with Kafka Streams, and rewrites `Output.csv` after every accepted aggregate update.

Expose same sorted report as JSON and downloadable CSV. Provide Angular summary screen that polls JSON every five seconds and links CSV download. Package backend, frontend, and a single-broker Kafka as containers. Kubernetes manifests deploy Kafka, one backend replica with persistent `/data` storage, and the frontend. Bootstrap/topic names stay configurable so a company cluster can replace the bundled broker.

## User Stories

1. As a business user, I want daily totals grouped by client and product, so that I can understand each client's net product position.
2. As a business user, I want long quantity reduced by short quantity, so that the displayed amount represents net transaction quantity.
3. As a business user, I want client information combined in a stable field order, so that each report row identifies the correct client account.
4. As a business user, I want product information combined in a stable field order, so that each report row identifies the correct future product.
5. As a business user, I want report rows sorted consistently, so that repeated downloads are easy to compare.
6. As a business user, I want signed integer totals without leading zeroes, so that amounts are readable and unambiguous.
7. As a business user, I want an `Output.csv` sample supplied with the solution, so that I can inspect expected results without running the application.
8. As an API consumer, I want the current summary as JSON, so that applications can process report rows directly.
9. As an API consumer, I want the current summary as a CSV attachment, so that users can download `Output.csv` through HTTP.
10. As an API consumer, I want JSON and CSV generated from the same current report state, so that both representations agree.
11. As an Angular user, I want a summary table, so that I can inspect client/product totals in a browser.
12. As an Angular user, I want the table refreshed every five seconds, so that new Kafka-processed transactions become visible quickly.
13. As an Angular user, I want visible refresh and error states, so that I know whether displayed data is current.
14. As an Angular user, I want a CSV download action, so that I can save the current report locally.
15. As a System A operator, I want existing input records consumed at startup, so that the initial daily report includes transactions already present.
16. As a System A operator, I want complete appended records consumed after startup, so that report totals update without a batch rerun.
17. As a System A operator, I want an append-only file contract, so that checkpointed ingestion remains deterministic.
18. As a System A operator, I want a final record accepted without a trailing newline, so that valid files are not rejected because of line termination.
19. As a data steward, I want blank lines ignored, so that harmless whitespace does not interrupt processing.
20. As a data steward, I want both 176-character and 303-character records accepted, so that supplied data and documented System A output both work.
21. As a data steward, I want positions 177 through 303 treated as unused filler, so that filler content cannot alter report keys or totals.
22. As a data steward, I want fixed-width values trimmed before key construction, so that padding does not create duplicate logical groups.
23. As a data steward, I want record code, length, numeric fields, and signs validated, so that malformed transactions cannot corrupt totals.
24. As a data steward, I want blank and plus signs treated as positive and minus signs treated as negative, so that quantity sign fields affect calculation consistently.
25. As an operations user, I want malformed records published to a dead-letter topic, so that processing can continue while rejected data remains diagnosable.
26. As an operations user, I want dead-letter records to include source position and validation error details, so that bad input can be traced and corrected.
27. As an operations user, I want ingestion offsets persisted, so that restarts resume from the last processed source position.
28. As an operations user, I want deterministic event identifiers and duplicate suppression, so that restart or redelivery does not double-count transactions.
29. As an operations user, I want file truncation or rewrite detected, so that append-only contract violations become visible instead of silently corrupting state.
30. As an operations user, I want `Output.csv` replaced atomically, so that readers never observe a partially written report.
31. As an operations user, I want health endpoints, so that Kubernetes can detect startup, readiness, and runtime failures.
32. As a Kafka operator, I want bootstrap servers and topic names externally configured, so that application code is portable between environments.
33. As a Kafka operator, I want a one-partition default for report aggregation, so that one active application instance owns a complete ordered snapshot.
34. As a Kafka operator, I want aggregate state backed by Kafka Streams state stores and changelogs, so that state can recover after restart.
35. As a Kafka operator, I want transactional or exactly-once processing enabled where supported, so that Kafka retries do not duplicate aggregate changes.
36. As a platform operator, I want backend and frontend container images, so that both applications can run consistently.
37. As a platform operator, I want Kubernetes manifests for backend and frontend, so that the solution can be deployed without inventing resource definitions.
38. As a platform operator, I want one backend replica mounted to persistent `/data` storage, so that file ownership, checkpoints, and report writing remain deterministic.
39. As a platform operator, I want Kafka included in Docker Compose and Kubernetes so the solution deploys as a complete stack, while bootstrap servers remain configurable.
40. As a developer, I want Java 21+ and Spring Boot, so that implementation meets required technology constraints.
41. As a developer, I want exact integer arithmetic, so that aggregation cannot lose precision through floating-point conversion.
42. As a developer, I want reusable parsing, aggregation, report, and API boundaries, so that each business rule has one implementation.
43. As a tester, I want supplied input to produce five known rows, so that end-to-end business behavior has an independent acceptance oracle.
44. As a tester, I want Kafka integration coverage, so that ingestion, deduplication, aggregation, dead-letter handling, and report updates are verified together.
45. As a tester, I want REST and generated CSV checked at the highest application seam, so that externally observable behavior is protected without coupling tests to internal classes.

## Implementation Decisions

- Backend uses Java 21+ with Spring Boot and Maven.
- Backend modules cover fixed-width parsing, append-only file ingestion, Kafka publishing, Kafka Streams aggregation, report generation, REST delivery, and runtime configuration.
- Parser validates record code `315` and accepts only records with 176 or 303 characters after line terminators are removed.
- For 303-character records, parser reads fields through position 176 and ignores positions 177 through 303 as filler regardless of filler content.
- Blank lines are ignored. Final record does not require a terminating newline.
- Client key joins trimmed CLIENT TYPE, CLIENT NUMBER, ACCOUNT NUMBER, and SUBACCOUNT NUMBER with `|` in that order.
- Product key joins trimmed EXCHANGE CODE, PRODUCT GROUP CODE, SYMBOL, and EXPIRATION DATE with `|` in that order.
- Long and short quantities use exact integer arithmetic. Blank or `+` signs are positive; `-` is negative; other signs are malformed.
- Transaction delta equals signed long quantity minus signed short quantity.
- File ingestion reads current records at startup, watches for complete appended lines, persists source offset metadata, and treats truncation/rewrite as an operational contract violation.
- Stable identifiers derived from source identity/offset and record content prevent duplicate aggregation after restart or redelivery.
- Malformed startup and appended records are published to configurable Kafka dead-letter topic and skipped; service continues processing valid records.
- Kafka Streams DSL groups events by client/product key, aggregates transaction deltas, and materializes queryable state with changelog recovery.
- Kafka processing uses transactional or exactly-once settings where supported. Topic names, bootstrap servers, and processing properties remain configurable. Docker Compose and Kubernetes deploy a single-broker Kafka by default.
- Topics default to one partition for complete ordered snapshot ownership by one backend replica.
- Report rows sort ascending by client components and then product components.
- `Output.csv` is rewritten after every accepted aggregate update through same-volume temporary file and atomic replacement.
- Runtime paths default to `/data/Input.txt` and `/data/Output.csv`; checkpoint and deduplication metadata also persist under `/data`. Local paths remain configurable.
- JSON contract is `GET /api/summary`, returning an array whose rows contain `clientInformation`, `productInformation`, and `totalTransactionAmount`.
- CSV contract is `GET /api/summary.csv`, returning `text/csv` with `Content-Disposition: attachment; filename="Output.csv"`.
- CSV headers are exactly `Client_Information,Product_Information,Total_Transaction_Amount`.
- JSON and CSV use same report state and canonical signed base-10 integer amounts without leading zeroes.
- Angular uses standalone application conventions and `HttpClient`, polls JSON every five seconds, renders summary table with refresh/error state, and downloads CSV through REST endpoint.
- Kubernetes deploys Kafka (KRaft single broker), one backend replica with persistent volume mounted at `/data`, plus frontend deployment and service. Docker Compose deploys the same three services locally. Bootstrap servers remain configurable.
- Spring Boot health endpoints provide Kubernetes liveness and readiness signals.
- Checked-in sample report is informational acceptance evidence and is not runtime output.

## Testing Decisions

- Tests assert external behavior and stable contracts rather than internal method structure.
- Primary acceptance seam is REST API plus generated `Output.csv`: submit known transactions, observe sorted JSON and CSV, and compare exact rows and headers.
- Secondary system seam is Kafka integration: verify startup ingestion, appended records, aggregation, duplicate suppression, restart recovery, dead-letter publication, and per-event report replacement with Testcontainers-backed Kafka.
- Narrow parser seam covers trust-boundary cases difficult to isolate at higher seams: 176/303 lengths, ignored filler, blank lines, unterminated final record, record code, signs, numeric validation, trimming, and fixed-width boundaries.
- Report tests cover canonical integer rendering, deterministic ordering, exact headers, escaping, and atomic replacement behavior.
- Spring MVC tests cover JSON shape, CSV media type, attachment filename, and agreement between representations.
- Angular tests cover five-second polling behavior, row rendering, visible error/refresh state, and CSV download target.
- Kubernetes and container verification covers backend/frontend image builds, bundled Kafka, manifest validation, probes, `/data` mount, and one backend replica.
- Supplied 717-record input must independently resolve to exact five-row sample. Expected fixture remains independent from production aggregation code.
- Repository contains no prior source or test suite, so no codebase testing precedent exists to reuse.

## Out of Scope

- Multi-broker production Kafka HA, rack-aware assignment, or a Kafka operator (Strimzi). The bundled broker is a complete single-node deploy.
- Multiple active backend replicas, distributed file ownership, Kubernetes leader election, or cross-partition interactive-query routing.
- Editing, truncating, or replacing `Input.txt` after ingestion; file is append-only.
- Historical reports, multi-day retention, date-range queries, authentication, authorization, or user management.
- Database persistence beyond Kafka Streams state/changelog and file checkpoint metadata.
- Server-Sent Events, WebSockets, or push-based Angular updates.
- User-configurable sorting, filtering, pagination, charting, or operational dashboard features.
- Automatic correction or replay of malformed dead-letter records.
- Inferring business meaning from filler fields or converting expiration date beyond preserving trimmed source value.

## Further Notes

- File specification states total record length 303 because positions 177–303 are filler. Supplied `Input.txt` contains 717 records of 176 characters, ending at OPEN CLOSE CODE. Supporting both lengths resolves this conflict without inventing missing values.
- File specification does not define quantity sign semantics, and supplied records leave sign fields blank. Blank/plus-positive and minus-negative behavior is explicit implementation policy.
- Requirements document appears to contain inconsistent client-number wording, while supplied input contains clients `1234` and `4321`; supplied data and business overview govern sample output.
- Offline sample `Output.csv`:

```csv
Client_Information,Product_Information,Total_Transaction_Amount
CL|1234|0002|0001,SGX|FU|NK|20100910,-52
CL|1234|0003|0001,CME|FU|N1|20100910,285
CL|1234|0003|0001,CME|FU|NK.|20100910,-215
CL|4321|0002|0001,SGX|FU|NK|20100910,46
CL|4321|0003|0001,CME|FU|N1|20100910,-79
```
