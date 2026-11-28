"""Serialize button commands against the same singleton row locked by the worker."""

from datetime import datetime, timedelta, timezone
import json
import uuid
from database.connection import connect
from simulator.kitchen import empty_day, CONFIG


def command(action, expected_session, expected_status, settings=None):
    with connect() as db:
        current = db.execute(
            "SELECT session_id FROM SimulationControl WITH (UPDLOCK,HOLDLOCK) WHERE id=1"
        ).fetchone()[0]
        status = "Ready"
        if current:
            status = db.execute(
                "SELECT status FROM SimulationSessions WHERE session_id=?", current
            ).fetchone()[0]
        if current != expected_session or status != expected_status:
            return False
        if action == "Settings":
            scenario, speed, factor = settings
            if (
                scenario not in CONFIG["arrival_seconds"]
                or speed not in (1, 2, 5, 10)
                or not 0.5 <= factor <= 2
            ):
                raise ValueError("Invalid settings")
            db.execute(
                "UPDATE SimulationControl SET scenario=?, speed=?, arrival_factor=?, revision=revision+1 WHERE id=1",
                scenario,
                speed,
                factor,
            )
        elif action == "Start" and status in ("Ready", "Completed"):
            session = str(uuid.uuid4())
            opening = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
            now = int(opening.timestamp())
            if current:
                closed = db.execute(
                    "SELECT closed_at FROM SimulationSessions WHERE session_id=?", current
                ).fetchone()[0]
                # The next business opens at noon on a later calendar day.
                closing_day = datetime.fromtimestamp(closed, timezone.utc)
                next_noon = closing_day.replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(
                    days=1
                )
                now = max(now, int(next_noon.timestamp()))
            db.execute(
                "INSERT INTO SimulationSessions(session_id,status,opened_at,sim_time,kitchen) VALUES(?,'Running',?,?,?)",
                session,
                now,
                now,
                json.dumps(empty_day(now)),
            )
            db.execute(
                "UPDATE SimulationControl SET session_id=?,demo_requested=1,revision=revision+1 WHERE id=1",
                session,
            )
        elif action == "Invalid demo" and status == "Running":
            db.execute("UPDATE SimulationControl SET demo_requested=1 WHERE id=1")
        else:
            transitions = {
                ("Running", "Pause"): "Paused",
                ("Paused", "Resume"): "Running",
                ("Running", "End"): "Closing",
                ("Paused", "End"): "Closing",
            }
            if (status, action) not in transitions:
                return False
            db.execute(
                "UPDATE SimulationSessions SET status=? WHERE session_id=?",
                transitions[status, action],
                current,
            )
            db.execute("UPDATE SimulationControl SET revision=revision+1 WHERE id=1")
    return True
