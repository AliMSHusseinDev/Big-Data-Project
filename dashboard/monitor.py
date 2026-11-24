import json
from urllib.request import build_opener, ProxyHandler
from kafka import KafkaConsumer, TopicPartition
import streamlit as st
from dashboard.data import read


def fetch(path):
    # These are private Compose service names.
    with build_opener(ProxyHandler({})).open(path, timeout=3) as response:
        return json.load(response)


def samples(consumer, topic):
    partition = TopicPartition(topic, 0)
    end = consumer.end_offsets([partition])[partition]
    start = consumer.beginning_offsets([partition])[partition]
    consumer.assign([partition])
    consumer.seek(partition, max(start, end - 60))
    messages = []
    if end > start:
        chunks = consumer.poll(timeout_ms=900, max_records=60)
        for message in chunks.get(partition, []):
            try:
                messages.append(json.loads(message.value))
            except (UnicodeError, ValueError):
                continue
    return start, end, messages


def monitor(state):
    st.subheader("Service connections")
    online = read(
        "SELECT CASE WHEN DATEDIFF(second,worker_seen,SYSUTCDATETIME()) < 30 THEN 1 ELSE 0 END alive FROM SimulationControl"
    ).iloc[0, 0]
    st.write("Simulator · " + ("Reporting" if online else "Heartbeat unavailable"))
    st.write("SQL Server · Query succeeded")
    for label, url in [
        ("NiFi", "http://nifi:8081/nifi-api/flow/process-groups/root"),
        ("Spark application", "http://spark:4040/api/v1/applications"),
    ]:
        try:
            data = fetch(url)
            if label == "NiFi":
                processors = data["processGroupFlow"]["flow"]["processors"]
                running = sum(p["component"]["state"] == "RUNNING" for p in processors)
                st.write(f"NiFi · {running} of {len(processors)} processors running")
            else:
                st.write(f"Spark · {data[0]['name']}")
        except Exception:
            st.write(label + " · Unavailable")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers="kafka:29092",
            enable_auto_commit=False,
            group_id=None,
            api_version_auto_timeout_ms=2500,
            request_timeout_ms=8000,
            session_timeout_ms=6000,
        )
        try:
            streams = {}
            for topic in ("raw", "cleaned", "invalid"):
                first, end, streams[topic] = samples(consumer, "kitchen." + topic)
                st.write(
                    f"kitchen.{topic} · partition 0 · earliest offset {first:,} · next offset {end:,}"
                )
        finally:
            consumer.close()
        raw = {r.get("event_id"): r for r in streams["raw"] if isinstance(r, dict)}
        for clean in reversed(streams["cleaned"]):
            if clean.get("item_id") and clean["event_id"] in raw:
                left, right = st.columns(2)
                left.markdown("**Raw event**")
                left.json(raw[clean["event_id"]])
                right.markdown("**After NiFi**")
                right.json(clean)
                break
        else:
            st.caption("Waiting for a matching item event in the latest 60 messages per topic.")
    except Exception as error:
        st.warning(f"Kafka samples unavailable ({type(error).__name__}).")
    if state.session_id:
        seen = read("SELECT COUNT(*) n FROM ProcessedEvents WHERE session_id=?", state.session_id).iloc[
            0, 0
        ]
        st.write(f"This day · {state.generated} valid events generated · {seen} unique events persisted")
    st.caption(
        "Offsets are partition positions. Retries can produce multiple messages for one event ID."
    )
    for label, url in [
        ("Kafka UI", "http://localhost:8085"),
        ("NiFi canvas", "http://localhost:8081/nifi"),
        ("Spark application", "http://localhost:4040"),
    ]:
        st.link_button(label, url)
