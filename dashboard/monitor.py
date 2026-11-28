import json
from datetime import datetime, timezone
from urllib.request import build_opener, ProxyHandler
from kafka import KafkaConsumer, TopicPartition
import streamlit as st
from dashboard.data import read
from dashboard.routing import check_routes, records_between, json_object


def fetch(path):
    # These are private Compose service names.
    with build_opener(ProxyHandler({})).open(path, timeout=3) as response:
        return json.load(response)


def samples(consumer, topic):
    partition = TopicPartition(topic, 0)
    if consumer.partitions_for_topic(topic) != {0}:
        raise RuntimeError(f"Missing partition for {topic}")
    end = consumer.end_offsets([partition])[partition]
    start = consumer.beginning_offsets([partition])[partition]
    messages = []
    for message in records_between(consumer, partition, max(start, end - 60), end):
        value = json_object(message.value)
        if topic == "kitchen.invalid" and "reason" not in value:
            value = {"original": (message.value or b"").decode(errors="replace")}
            reason = dict(message.headers or []).get("record.error.message")
            if reason:
                value["reason"] = reason.decode(errors="replace")
        messages.append(value)
    return start, end, messages


def monitor(session):
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
            auto_offset_reset="none",
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
            if session and st.button("Compare retained messages for this day"):
                totals, differences = check_routes(consumer, session)
                st.session_state["route_check"] = (
                    session,
                    datetime.now(timezone.utc),
                    totals,
                    differences,
                )
        finally:
            consumer.close()
        raw = {r["event_id"]: r for r in streams["raw"] if isinstance(r.get("event_id"), str)}
        st.markdown("**Recent Kafka samples (all days)**")
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
        if streams["invalid"]:
            st.markdown("**Latest rejection on kitchen.invalid**")
            st.json(streams["invalid"][-1])
        result = st.session_state.get("route_check")
        if result and result[0] == session:
            _, checked, (raw_count, cleaned, invalid), differences = result
            st.write(
                f"Checked at {checked:%H:%M:%S} UTC · Raw {raw_count:,} · Cleaned {cleaned:,} · Invalid {invalid:,}"
            )
            if differences:
                st.warning(
                    f"{len(differences)} identifiers have different input and output counts. Check again after NiFi drains."
                )
                st.dataframe(differences[:50], hide_index=True)
            elif raw_count:
                st.success(
                    "Input and output counts match for each retained event. The check includes rejected records."
                )
            else:
                st.info("No retained Kafka messages were found for this day.")
    except Exception as error:
        st.warning(f"Kafka samples unavailable ({type(error).__name__}).")
    if session:
        row = read(
            """SELECT generated, (SELECT COUNT(*) FROM ProcessedEvents WHERE session_id=s.session_id) AS seen
                      FROM SimulationSessions s WHERE session_id=?""",
            session,
        ).iloc[0]
        st.write(
            f"This day · {row.generated} valid events generated · {row.seen} unique events persisted"
        )
    st.caption(
        "Offsets are partition positions. Retries can produce multiple messages for one event ID."
    )
    for label, url in [
        ("Kafka UI", "http://localhost:8085"),
        ("NiFi canvas", "http://localhost:8081/nifi"),
        ("Spark application", "http://localhost:4040"),
    ]:
        st.link_button(label, url)
