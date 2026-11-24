"""In a dedicated QA stack, send controlled records through the actual pipeline and restart Spark between phases."""

import json
import sys
import uuid
from kafka import KafkaProducer
from database.connection import connect
from tests.full_pipeline import wait_for

# The stable session comes from the test runner and never becomes the simulator's active day.
if __name__ == "__main__":
    phase, session = sys.argv[1:]
    uuid.UUID(session)
    if phase == "start":
        with connect() as db:
            db.execute(
                """INSERT INTO SimulationSessions(session_id,status,opened_at,sim_time,kitchen)
                          VALUES(?,'Closing',1767268800,1767268980,'{}')""",
                session,
            )
    records = [
        ("PLACED", "12:00:00"),
        ("PREPARING", "12:01:00"),
        ("CLOCK", "12:03:00"),
        ("READY", "12:04:00"),
        ("FINAL", "12:04:00"),
    ]
    if phase == "finish":
        with connect() as db:
            db.execute(
                "UPDATE SimulationSessions SET expected_events=5,generated=5 WHERE session_id=?", session
            )
    producer = KafkaProducer(bootstrap_servers="kafka:29092", acks="all")
    indices = [0, 1, 2] if phase == "start" else [1, 0, 3, 3, 4]
    for index in indices:
        kind, time = records[index]
        event = dict(
            session_id=session,
            event_id=session + kind,
            event_type=kind,
            event_time="2026-01-01T" + time + "Z",
        )
        if kind not in ("CLOCK", "FINAL"):
            event.update(
                item_id=session + "-I",
                order_id=session + "-O",
                order_size=1,
                item_name=" COLA ",
                category=" drinks ",
                station_id="drinks",
                sla_seconds="75",
            )
        producer.send("kitchen.raw", key=session.encode(), value=json.dumps(event).encode()).get(
            timeout=10
        )
    producer.close()

    def expected_result():
        with connect() as db:
            row = db.execute(
                "SELECT status,delay_seconds FROM SLAAlerts WHERE session_id=?", session
            ).fetchone()
            if not row:
                return False
            if phase == "start":
                return tuple(row) == ("Active", 45)
            state = db.execute(
                "SELECT status FROM SimulationSessions WHERE session_id=?", session
            ).fetchone()[0]
            count = db.execute(
                "SELECT COUNT(*) FROM ProcessedEvents WHERE session_id=?", session
            ).fetchone()[0]
            return tuple(row) == ("Resolved", 105) and count == 5 and state == "Completed"

    wait_for(expected_result, 150)
    print(f"PASS: {phase} replay fixture")
