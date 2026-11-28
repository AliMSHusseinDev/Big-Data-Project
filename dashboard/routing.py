"""Compare retained Kafka records for one business day without changing consumer offsets."""

from collections import Counter
from hashlib import sha256
import json
import time

from kafka import TopicPartition


def records_between(consumer, partition, start, end):
    consumer.assign([partition])
    consumer.seek(partition, start)
    deadline = time.monotonic() + 15
    while consumer.position(partition) < end:
        if time.monotonic() > deadline:
            raise TimeoutError("Kafka scan did not finish")
        for message in consumer.poll(timeout_ms=500).get(partition, []):
            if message.offset < end:
                yield message


def json_object(text):
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def check_routes(consumer, session):
    parts = [TopicPartition("kitchen." + name, 0) for name in ("raw", "cleaned", "invalid")]
    consumer.assign(parts)
    for part in parts:
        if consumer.partitions_for_topic(part.topic) != {part.partition}:
            raise RuntimeError(f"Missing partition for {part.topic}")
    starts = consumer.beginning_offsets(parts)
    ends = consumer.end_offsets(parts)
    counts = []
    for part in parts:
        events = Counter()
        for message in records_between(consumer, part, starts[part], ends[part]):
            body = (message.value or b"").decode(errors="replace")
            event = json_object(body)
            if part.topic == "kitchen.invalid" and isinstance(event.get("original"), str):
                body = event["original"]
                event = json_object(body)
            key = (message.key or b"").decode(errors="replace")
            owner = event.get("session_id")
            belongs = (
                str(owner).strip() == session
                if owner
                else key == session or key.startswith(session + "-")
            )
            if not belongs:
                continue
            identity = event.get("event_id")
            if not isinstance(identity, str) or not identity.strip():
                identity = "Payload " + sha256(body.encode()).hexdigest()[:16]
            source = dict(message.headers or []).get("kafka.offset")
            offset = int(source) if source and source.isdigit() else None
            if part.topic == "kitchen.raw":
                offset = message.offset
            events[identity.strip(), offset] += 1
        counts.append(events)
    # Older publishers did not attach the raw offset. Compare those days by ID.
    if any(offset is None for output in counts[1:] for _, offset in output):
        for i, events in enumerate(counts):
            by_id = Counter()
            for (identity, _), n in events.items():
                by_id[identity, None] += n
            counts[i] = by_id
    raw, cleaned, invalid = counts
    outputs = cleaned + invalid
    differences = [
        {
            "Event ID or payload": key[0],
            "Raw offset": key[1],
            "Raw": raw[key],
            "Cleaned": cleaned[key],
            "Invalid": invalid[key],
        }
        for key in (raw.keys() | outputs.keys())
        if raw[key] != outputs[key]
    ]
    return [sum(c.values()) for c in counts], sorted(
        differences, key=lambda row: row["Event ID or payload"]
    )
