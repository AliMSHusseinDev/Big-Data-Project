import pandas as pd
from database.connection import connect


def read(sql, *params):
    with connect() as db:
        cursor = db.cursor().execute(sql, *params)
        columns = [c[0] for c in cursor.description]
        return pd.DataFrame.from_records([tuple(row) for row in cursor.fetchall()], columns=columns)


def control_state():
    return read("""SELECT c.*,COALESCE(s.status,'Ready') status,s.sim_time,s.generated
                   FROM SimulationControl c LEFT JOIN SimulationSessions s ON c.session_id=s.session_id""").iloc[
        0
    ]


def day_data(session):
    return {
        table: read(f"SELECT * FROM {table} WHERE session_id=?", session)
        for table in (
            "Orders",
            "OrderItems",
            "StationMetrics",
            "CategoryMetrics",
            "SLAAlerts",
            "MetricHistory",
        )
    }


def with_times(frame):
    result = frame.copy()
    for name in ("placed_at", "started_at", "ready_at", "alert_time", "sample_time"):
        if name in result:
            result[name] = pd.to_datetime(result[name], unit="s", utc=True)
    return result
