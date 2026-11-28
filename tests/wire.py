"""Run in the isolated kitchen-wire stack. Exercise real Kafka and the running NiFi flow."""

import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from dashboard.routing import check_routes
from simulator.worker import invalid_demo

if __name__ == "__main__":
    session = str(uuid.uuid4())
    item = dict(
        session_id=session,
        order_id=session + "-O",
        order_size=1,
        item_id=session + "-I",
        item_name=" cola ",
        category=" drinks ",
        station_id=" drinks ",
        sla_seconds="75",
    )
    producer = KafkaProducer(bootstrap_servers="kafka:29092", acks="all", enable_idempotence=True)
    consumer = KafkaConsumer(bootstrap_servers="kafka:29092", group_id=None, enable_auto_commit=False)
    parts = [TopicPartition("kitchen." + name, 0) for name in ("cleaned", "invalid")]
    consumer.assign(parts)
    for p, end in consumer.end_offsets(parts).items():
        consumer.seek(p, end)
    messages = []
    for kind, minute in [("placed", 0), ("preparing", 1), ("ready", 3)]:
        messages.append(
            json.dumps(
                dict(
                    item,
                    event_id=session + kind,
                    event_type=kind,
                    event_time=f"2026-01-01T12:0{minute}:00+00:00",
                )
            ).encode()
        )
    for kind in ("CLOCK", "FINAL"):
        messages.append(
            json.dumps(
                dict(
                    session_id=session,
                    event_id=session + kind,
                    event_type=kind,
                    event_time="2026-01-01T12:03:00Z",
                )
            ).encode()
        )
    offsets = set()
    for payload in [
        messages[0],
        messages[1],
        messages[3],
        messages[2],
        messages[2],
        messages[4],
        (session + "{bad-json").encode(),
        json.dumps(invalid_demo(session)).encode(),
    ]:
        sent = producer.send("kitchen.raw", key=item["item_id"].encode(), value=payload).get(timeout=15)
        offsets.add(sent.offset)
    results = {"cleaned": [], "invalid": []}
    deadline = time.monotonic() + 100
    while time.monotonic() < deadline:
        for partition, records in consumer.poll(timeout_ms=500).items():
            for message in records:
                if session.encode() not in message.value:
                    continue
                offsets.remove(int(dict(message.headers)["kafka.offset"]))
                record = json.loads(message.value)
                if partition.topic == "kitchen.cleaned":
                    # The publisher takes the Kafka key from NiFi's message.key attribute.
                    assert message.key.decode() == (record.get("item_id") or session)
                results[partition.topic.split(".")[1]].append(record)
        if len(results["cleaned"]) >= 6 and len(results["invalid"]) >= 2:
            break
    assert not offsets, offsets
    assert check_routes(consumer, session) == ([8, 6, 2], [])
    producer.close()
    consumer.close()
    assert {r["event_type"] for r in results["cleaned"] if not r.get("item_id")} == {"CLOCK", "FINAL"}
    clean = [r for r in results["cleaned"] if r.get("item_id")]
    assert len(clean) == 4 and all(
        r["item_name"] == "Cola"
        and r["station_id"] == "DRINKS"
        and r["category"] == "Drinks"
        and r["sla_seconds"] == 75
        for r in clean
    ), results
    assert {r["event_type"] for r in clean} == {"PLACED", "PREPARING", "READY"}
    assert len({r["event_id"] for r in clean}) == 3
    assert all(r["reason"] and session in r["original"] for r in results["invalid"])
    assert any(r["reason"] == "Unknown event_type" for r in results["invalid"])
    print(
        "PASS: 8 raw = 6 cleaned + 2 rejected. All 8 raw offsets matched once, including the replay and opening demo."
    )
