import json
import time
from kafka import KafkaProducer
from database.connection import connect
from simulator.kitchen import advance


def tick(previous, previous_revision):
    with connect() as db:
        control = db.execute(
            "SELECT * FROM SimulationControl WITH (UPDLOCK,HOLDLOCK) WHERE id=1"
        ).fetchone()
        db.execute("UPDATE SimulationControl SET worker_seen=SYSUTCDATETIME() WHERE id=1")
        now = time.monotonic()
        session = control.session_id
        if not session:
            return now, control.revision, None, []
        row = db.execute(
            "SELECT status,kitchen,pending FROM SimulationSessions WHERE session_id=?", session
        ).fetchone()
        pending = json.loads(row.pending)
        if not pending and row.status in ("Running", "Closing"):
            day = json.loads(row.kitchen)
            elapsed = now - previous if control.revision == previous_revision else 0
            pending = advance(
                day,
                session,
                row.status,
                elapsed,
                control.speed,
                control.scenario,
                control.arrival_factor,
            )
            if control.demo_requested:
                pending.append(
                    {"session_id": session, "event_type": "BROKEN", "example": "isolated invalid demo"}
                )
                db.execute("UPDATE SimulationControl SET demo_requested=0 WHERE id=1")
            db.execute(
                """UPDATE SimulationSessions SET kitchen=?,pending=?,sim_time=?,generated=?,expected_events=?
                          WHERE session_id=?""",
                json.dumps(day),
                json.dumps(pending),
                int(day["now"]),
                day["events"],
                day["events"] if day["finished"] else None,
                session,
            )
    return now, control.revision, session, pending


def run():
    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        acks="all",
        max_in_flight_requests_per_connection=1,
        key_serializer=str.encode,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    previous, revision = time.monotonic(), None
    while True:
        previous, revision, session, pending = tick(previous, revision)
        for record in pending:
            key = record.get("item_id", session)
            producer.send("kitchen.raw", key=key, value=record).get(timeout=30)
        if pending:
            # A crash before this commit resends the same IDs on restart.
            with connect() as db:
                db.execute("UPDATE SimulationSessions SET pending='[]' WHERE session_id=?", session)
        time.sleep(0.4)


if __name__ == "__main__":
    run()
