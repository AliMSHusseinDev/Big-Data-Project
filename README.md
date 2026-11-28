# Real-Time Restaurant Kitchen Order Monitoring

A five-station restaurant streams item lifecycle events through **Kafka → NiFi → Kafka → Spark Structured Streaming → SQL Server**. Streamlit shows the results and controls a separate simulator. The kitchen waits for **Start Simulation** before accepting its first customer.

## Run on Windows

1. Install Docker Desktop with the WSL 2 Linux container engine on an x86-64 Windows machine. Use Docker Compose 2.24.4 or later. Allow roughly 10–12 GB RAM and 25 GB free disk for the images and persistent volumes. SSMS is optional.
2. Clone or extract the complete project. Double-click **Start_Project.bat**. Keep the console open while Docker downloads images, builds drivers, initializes the schema, deploys the NiFi flow and starts the streaming query. The launcher generates a unique local SQL password in `.env` on first use.
3. The launcher checks the streaming job, NiFi processors, topic metadata, simulator heartbeat, SQL tables and web endpoints. It opens the dashboard after those checks pass.
4. Click **Start Simulation**. Set speed to **10×** for a demonstration. **Pause** freezes the virtual clock. **End Simulation** closes arrivals and drains orders. Wait for **Completed** before opening the final report or double-clicking **Stop_Project.bat**. **Start New Day** preserves the previous day.
5. On subsequent launches use the same folder and `.env`. Normal stop/start preserves volumes. Back up `.env` with local data. A new password in that file will not change the login of an existing database.

| Interface | Host address | What to inspect |
|---|---|---|
| Dashboard | `http://localhost:8501` | Overview, controls, reports and live SQL data |
| NiFi | `http://localhost:8081/nifi/` | Running processors, connections, queues and controller services |
| Kafka UI | `http://localhost:8085` | Kitchen cluster, topics, messages, partitions and consumer groups |
| Spark application | `http://localhost:4040` | Jobs, SQL plans and Structured Streaming queries |
| SQL Server | `localhost,1433` | Database Engine in SSMS, SQL Server Authentication, login `sa`, password from local `.env`. Enable **Trust server certificate** for the local instance. |
| Kafka host listener | `localhost:9092` | Optional host Kafka client. Containers use `kafka:29092`. |

All published ports bind to loopback. The Compose network is for local teaching. SQL Server uses the Developer edition. NiFi's canvas uses local HTTP. Spark runs a two-thread local driver with its own application UI.

## Course lab and existing installations

The project uses Kafka 4.3.2 (Scala 2.13) in KRaft mode and NiFi 2.12.0. The handbook covers the Week 9 Kafka questions in Member 2, the Week 10 NiFi questions in Member 3, and the Spark transformations in Member 4.

The [sample dataset](dataset/) contains two chronological JSONL files from the same restaurant simulator used live. Run `python -m dataset.generate` to regenerate them with the fixed seed. After building the images, run the command below from the project root to read both files, print Spark's physical plan and count completed items by category.

```sh
docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --master local[2] /app/dataset/analyze.py
```

**Updating from Kafka 3.8.1 / NiFi 1.28.1.** On the old version, end the day, wait for Completed, and confirm all NiFi connections have zero queued FlowFiles and kitchen-etl has caught up with kitchen.raw. Stop the stack and back up the local .env and volumes before updating. This version reuses the Kafka log, SQL database and Spark checkpoint. It initializes separate NiFi 2 configuration, FlowFile and content volumes because the processor types and properties changed between major versions. Keep the old NiFi volumes for recovery. Start_Project.bat will pull the two updated images. Do not downgrade an upgraded Kafka log in place.

## Linux or cloud

From the checkout, with Python 3 and Docker Compose installed:

```sh
python3 scripts/configure.py
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 600
docker compose exec -T dashboard python -m scripts.readiness
```

Managed builders with an organization-specific TLS trust bundle can run the two `docker build` commands in [the test report](docs/Test_Report.md) before `docker compose up`. Allow the Microsoft Container Registry layer host returned by the SQL image pull. On Linux the Spark UID needs read and traversal access to the checkout and the Python modules it loads, including `dataset/` for the course exercise. NiFi needs read access to `config/` and `nifi/start-local.sh`.

## Live demonstration

1. Start the stack. Show the verified endpoint summary, idle kitchen and empty current-day state. Start a day and watch a single order split into individual items.
2. Open Kafka UI → Kitchen → Topics → kitchen.raw → Messages. Copy an event ID. Open the NiFi canvas and follow **Read raw orders → Normalize and check menu → Validate event schema → Publish cleaned**. Inspect that ID in kitchen.cleaned, or use the matched pair in Data Pipeline Monitor. Each new day sends one separate record with event type `BROKEN`. Inspect the original and the rejection reason in kitchen.invalid. Simulation Control can send another example.
3. Open the Spark UI's Structured Streaming tab. Inspect the Kafka source, trigger progress and jobs. In SSMS connect to RestaurantDB, expand Tables, and run the inspection queries in the handbook. Compare the stored items with the dashboard.
4. Use **Normal Traffic** at **5×** to watch ordinary service. A small share of items need a second preparation. Use **Rush Hour** for busier queues or **Kitchen Delay** for more remakes, then press **Apply settings**. On **SLA Alerts**, compare actual prep seconds with the limit. Overview shows total, active and resolved violations and the share of orders with a late item. Once the day is Completed, every late item is resolved.
5. Pause and record the simulated time. Wait, refresh, then resume the same day. Press End. Watch Closing drain the backlog. On Completed, open End-of-Day Analysis and compare category preparation, observed peaks and busiest station.
6. In **Data Pipeline Monitor**, choose the business date and press **Compare retained messages for this day** after NiFi drains. In a clean run, raw equals cleaned plus invalid. The check compares individual records, counts repeated copies and lists differences. Both publishers attach the partition-0 raw offset in the `kafka.offset` header. Older records can still be matched by event ID. Kafka UI topic totals span business days and can change while you read them.
7. Start a new day. Select the old date in Analytics or End-of-Day Analysis to see its saved results.

## Tests and troubleshooting

Run the simulator tests with `python -m unittest discover -s tests -p test_simulation.py -v`. To run the full SQL, replay, restart, control and dashboard acceptance tests on Windows after startup, use `powershell -ExecutionPolicy Bypass -File scripts/test-stack.ps1`. This creates a separate Compose project with its own volumes and no published ports. Allow sufficient memory for two stacks, or stop the demonstration stack first.

If startup stops, run `docker compose ps -a`, then `docker compose logs --tail 80 SERVICE`. Successful one-shot services (topics, schema, flow) exit with code 0. A Closing session waits for its expected valid event IDs, FINAL and ready orders. Inspect NiFi queues, rejected messages and Spark logs before troubleshooting the dashboard. If a port is occupied, stop the conflicting local service.

[Six-member handbook (PDF)](docs/Restaurant_Kitchen_Monitoring_Documentation.pdf) · [Editable handbook](docs/Handbook.md) · [Test results and exact reproduction commands](docs/Test_Report.md)
