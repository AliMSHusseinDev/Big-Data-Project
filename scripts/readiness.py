"""Functional checks of the local Compose services. Run inside the dashboard container."""

from urllib.request import build_opener, ProxyHandler
import json
from kafka import KafkaConsumer
from database.connection import connect

if __name__ == "__main__":
    with connect() as db:
        for table in (
            "SimulationSessions",
            "SimulationControl",
            "Orders",
            "OrderItems",
            "ProcessedEvents",
            "StationMetrics",
            "CategoryMetrics",
            "SLAAlerts",
            "MetricHistory",
        ):
            db.execute("SELECT TOP 1 * FROM " + table).fetchone()
        assert db.execute(
            "SELECT id FROM SimulationControl WHERE worker_seen>DATEADD(second,-30,SYSUTCDATETIME())"
        ).fetchone()
    print("SQL schema and simulator heartbeat verified")
    consumer = KafkaConsumer(bootstrap_servers="kafka:29092", group_id=None)
    for topic in ("kitchen.raw", "kitchen.cleaned", "kitchen.invalid"):
        assert consumer.partitions_for_topic(topic) == {0}, topic
    consumer.close()
    http = build_opener(ProxyHandler({}))
    flow = json.load(http.open("http://nifi:8081/nifi-api/flow/process-groups/root", timeout=8))
    processors = flow["processGroupFlow"]["flow"]["processors"]
    assert len(processors) == 5 and all(
        p["component"]["state"] == "RUNNING" and p["component"]["validationStatus"] == "VALID"
        for p in processors
    )
    service_list = json.load(
        http.open("http://nifi:8081/nifi-api/flow/process-groups/root/controller-services", timeout=8)
    )
    enabled = {
        entry["component"]["name"]
        for entry in service_list["controllerServices"]
        if entry["component"]["state"] == "ENABLED"
    }
    assert {"Kitchen broker", "Kitchen JSON reader", "Kitchen JSON writer"} <= enabled
    assert (
        json.load(http.open("http://spark:4040/api/v1/applications", timeout=5))[0]["name"]
        == "Kitchen order monitoring"
    )
    assert json.load(http.open("http://kafka-ui:8080/api/clusters", timeout=8))[0]["status"] == "online"
    assert http.open("http://dashboard:8501/_stcore/health", timeout=5).read().strip() == b"ok"
    print(
        "Broker, three topics, five running NiFi processors, Spark UI, Kafka UI and dashboard verified"
    )
