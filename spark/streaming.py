"""One streaming writer. Kafka offsets are checkpointed after foreachBatch returns."""

import json
import os
from datetime import datetime
from pathlib import Path
from simulator.kitchen import STATIONS
from pyspark.sql import SparkSession, functions as F
from spark.analytics import parse_events, item_states, reports

URL = "jdbc:sqlserver://sql:1433;databaseName=RestaurantDB;encrypt=true;trustServerCertificate=true"
DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def execute(db, sql, rows):
    statement = db.prepareStatement(sql)
    try:
        for row in rows:
            for index, value in enumerate(row, 1):
                statement.setString(index, None if value is None else str(value))
            statement.addBatch()
        statement.executeBatch()
    finally:
        statement.close()


def insert_rows(db, table, columns, rows):
    if rows:
        placeholders = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        execute(db, sql, rows)


def store_batch(messages, batch_id):
    records = messages.selectExpr("CAST(value AS STRING) AS payload").collect()
    if not records:
        return
    by_session = {}
    for row in records:
        event = json.loads(row.payload)
        timestamp = int(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).timestamp())
        session = event["session_id"]
        by_session.setdefault(session, []).append(
            (event["event_id"], event["event_id"], session, timestamp, row.payload)
        )
    # Properties carry credentials outside the JDBC URL and outside log messages.
    props = spark._jvm.java.util.Properties()
    for key, value in credentials.items():
        props.setProperty(key, value)
    db = spark._jvm.com.microsoft.sqlserver.jdbc.SQLServerDriver().connect(URL, props)
    db.setAutoCommit(False)
    try:
        for session, rows in by_session.items():
            execute(
                db,
                """IF NOT EXISTS (SELECT 1 FROM ProcessedEvents WHERE event_id=?)
                          INSERT INTO ProcessedEvents(event_id,session_id,event_time,payload) VALUES(?,?,?,?)""",
                rows,
            )
            db.commit()  # Spark's JDBC readers use independent connections.
            refresh_day(db, session)
            db.commit()
    finally:
        db.close()


def refresh_day(db, session):
    history = spark.read.jdbc(URL, "dbo.ProcessedEvents", properties=credentials).where(
        F.col("session_id") == session
    )
    clock = history.agg(F.max("event_time")).first()[0]
    events = parse_events(history.select(F.col("payload").alias("value")))
    items = item_states(events, clock).cache()
    try:
        tables = reports(items)
        stations = spark.createDataFrame([(s["id"],) for s in STATIONS], ["station_id"])
        tables["StationMetrics"] = (
            stations.join(tables["StationMetrics"], "station_id", "left")
            .withColumn("session_id", F.lit(session))
            .fillna(0, ["waiting", "preparing", "completed", "violations"])
        )
        # Replace this day's snapshot in one transaction, children before parents on delete.
        for name in reversed(tables):
            execute(db, f"DELETE FROM {name} WHERE session_id=?", [(session,)])
        for name, frame in tables.items():
            rows = frame.collect()
            insert_rows(db, name, frame.columns, rows)
            if name == "StationMetrics":
                execute(
                    db,
                    "DELETE FROM MetricHistory WHERE session_id=? AND sample_time=?",
                    [(session, clock)],
                )
                insert_rows(
                    db,
                    "MetricHistory",
                    ["session_id", "sample_time", "station_id", "waiting", "preparing"],
                    [(session, clock, r.station_id, r.waiting, r.preparing) for r in rows],
                )
        execute(
            db,
            """UPDATE SimulationSessions SET status='Completed',closed_at=?
                      WHERE session_id=? AND status='Closing' AND expected_events IS NOT NULL
                      AND expected_events=(SELECT COUNT(*) FROM ProcessedEvents WHERE session_id=?)
                      AND EXISTS(SELECT 1 FROM ProcessedEvents WHERE session_id=? AND JSON_VALUE(payload,'$.event_type')='FINAL')
                      AND NOT EXISTS(SELECT 1 FROM Orders WHERE session_id=? AND status<>'READY')""",
            [(clock, session, session, session, session)],
        )
    finally:
        items.unpersist()


if __name__ == "__main__":
    credentials = {"user": "sa", "password": os.environ["MSSQL_SA_PASSWORD"], "driver": DRIVER}
    spark = (
        SparkSession.builder.appName("Kitchen order monitoring")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.read.jdbc(URL, "dbo.SimulationControl", properties=credentials).select("id").first()
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "kitchen.cleaned")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )
    query = (
        source.writeStream.foreachBatch(store_batch)
        .queryName("kitchen_orders")
        .option("checkpointLocation", "/checkpoints/kitchen-orders")
        .trigger(processingTime="4 seconds")
        .start()
    )
    while not query.awaitTermination(2):
        # First progress confirms offset discovery; update liveness even when the kitchen is idle.
        if query.lastProgress is not None:
            Path("/tmp/kitchen-stream-live").touch()
