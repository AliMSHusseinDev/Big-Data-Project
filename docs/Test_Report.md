# Verification report

Linux x86-64 cloud run, 24 November 2026. Docker Engine 29.9.2, Compose 2.40.3. The tested transport stack used real services in an isolated Compose project. The Windows launcher and the SQL-backed pipeline still require target-host acceptance.

| Check | Expected | Observed | Status |
|---|---|---|---|
| Compose configuration | Service dependencies, ports and volumes resolve | Production config and isolated test overlay parsed. Kafka, NiFi, Kafka UI and dashboard started. | Passed |
| Application image build | Python dependencies and Microsoft ODBC driver installed | Built kitchen-python:1. pyodbc enumerated ODBC Driver 18 for SQL Server; Streamlit 1.45.1, Plotly 6.1.2, pandas 2.2.3 and kafka-python 2.2.15 imported. | Passed |
| Spark image | Compatible Spark/Scala connector, JDBC JAR and Java runtime | Built kitchen-spark:1; real Spark Kafka query read the broker. JDBC artifact downloaded and checksum verified. | Passed |
| Broker and inspector | Three one-partition topics, online cluster | Kafka metadata returned all three topics; Kafka UI reported an online cluster with one broker and three online partitions. | Passed |
| NiFi initialization | Exactly five valid running processors and two enabled JSON services | Created flow; repeated initialization reused entities. After a service restart the isolated flow reinitialized successfully. | Passed |
| Simulator scheduling | Capacity, queue, lifecycle, varied traffic, pause, speed change and closing | Three standard-library tests passed. All four speeds exercised. A seeded rush generated queued work, drained all items and emitted one final marker. | Passed |
| Spark analytics | Three waiting and two preparing; 480 s category mean; alerts at strict SLA crossing; stable replay | Two PySpark tests passed. At 480 s elapsed with 360 s SLA the active delay was 120 s. The 360/480/600 s fixture produced two resolved violations and one item at the threshold. | Passed |
| Real Kafka → NiFi → Kafka | Normalize valid values; route malformed JSON with reason and original bytes represented as text | Latest run received 4 clean lifecycle messages (3 unique IDs), 2 control messages, and 1 rejected malformed message. Paired JSON saved in etl-example.json. | Passed |
| Broker and flow restart | Existing messages survive, clean flow accepts additional messages | Stopped and restarted Kafka and NiFi. First test's four cleaned messages plus second test's six remained readable. | Passed |
| Spark streaming and checkpoint | Separate micro-batches recover correct item state; restart reads no duplicate offsets | A real Kafka Structured Streaming query consumed 10 cleaned messages at one offset per batch, wrote a test-only Parquet history, computed 120 s prep, one completed item and one violation per session. Reopening the checkpoint left 10 records. | Passed |
| Dashboard HTTP and unavailable state | Valid Streamlit health response; clear error on missing database | HTTP health returned ok. Streamlit AppTest opened the real app with SQL unavailable and observed the database error message with no uncaught exception. | Passed |
| SQL Server image / full Compose start | Pull CU18 layers and start SQL Developer | Manifest resolved; Microsoft's redirect to centralus.data.mcr.microsoft.com received a cloud network denial. Both image pull and full launch failed before SQL startup. | Blocked |
| SQL schema, JDBC write/replay, ODBC login and full worker | Real SQL persistence; active then resolved alert; only finish after final events commit | SQL service prerequisite blocked. Production JDBC sink, worker transactions and completion barrier still require live acceptance. | Unrun |
| SQL-backed charts, history, restart and UI controls | Two durable days; all pages render actual results | Dedicated real-stack suite provided. Requires the SQL image. | Unrun |
| Windows BAT, Docker Desktop browser launch, SSMS login | One click twice, endpoints verified, inspect real tables | Linux cloud machine only. | Unrun |
| Student documentation | Six member sections, actual source listings and authentic screenshots | Generated A4 PDF, checked extracted text, member headings and rendered sample pages. NiFi, Kafka UI and Spark screenshots came from the running local services. | Passed |

## Cloud settings retry

On 24 November the saved configuration contained `centralus.data.mcr.microsoft.com`. The running instance reported enforced revision 1 with that host absent. A normal Docker pull and an escalated retry each failed after six attempts with `Forbidden`; the HTTPS proxy also returned 403 for the layer host. SQL initialization, JDBC writes, database-backed dashboard values, controls, historical sessions and full-stack restart remain unverified. The acceptance suite now rejects Streamlit error panels and compares rendered order and completed-item counts with the SQL rows for the completed day.

## Reproduce the independent checks

From the repository root in Bash, on a Docker host:

```sh
python3 scripts/configure.py
docker compose config --quiet
docker compose build
python3 -m unittest discover -s tests -p test_simulation.py -v
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit /app/tests/test_analytics.py

wire() { docker compose -p kitchen-wire -f docker-compose.yml -f tests/isolated.yml "$@"; }
wire up -d flow
wire wait flow
wire run --rm --no-deps flow
wire run --rm --no-deps dashboard python -m tests.wire
wire stop kafka nifi
wire up -d flow
wire wait flow
wire run --rm --no-deps dashboard python -m tests.wire
wire run --rm --no-deps spark /opt/spark/bin/spark-submit /app/tests/stream_probe.py
wire stop
```

Each wire test creates a different session. The historical message count depends on how many fixtures have previously run; the probe asserts the per-session result and the unchanged checkpoint count. Its test sink allows the live transport and Spark calculations to be tested independently of SQL Server.

For this managed cloud builder, the following equivalent builds use the host's trusted public CA bundle during downloads:

```sh
docker build --secret id=build_ca,src=/etc/ssl/certs/ca-certificates.crt -t kitchen-python:1 .
docker build --secret id=build_ca,src=/etc/ssl/certs/ca-certificates.crt -f spark/Dockerfile -t kitchen-spark:1 .
```

## Finish acceptance on Windows

1. Enable Docker Desktop Linux containers and run Start_Project.bat. Confirm all reported endpoints are accessible and the simulator opens in Ready. Run it a second time and verify the same five NiFi processors.
2. In PowerShell from the project root, run `powershell -ExecutionPolicy Bypass -File scripts/test-stack.ps1`. This creates kitchen-e2e with independent named volumes. It runs functional readiness checks, a controlled active alert, Spark restart, replay across triggers, completion, UI rendering, a new session and SQL persistence after a full restart. Stop the demonstration stack first if RAM is constrained.
3. Start the demonstration stack again if needed. Follow the README live demonstration, verify controls through the browser, close and reopen the tab while running, and check the stored rows from SSMS. Record observed results on the target host.
