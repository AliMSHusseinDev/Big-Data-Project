"""Spark transformations for the complete event history of one business day."""

from pyspark.sql import functions as F

EVENT_SCHEMA = (
    "event_id string, session_id string, event_type string, event_time string, "
    "item_id string, order_id string, item_name string, category string, station_id string, "
    "order_size int, sla_seconds int"
)


def parse_events(messages):
    return messages.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select(
        "e.*"
    )


def item_states(events, clock):
    # min per lifecycle step is independent of arrival order and delivery count.
    timeline = (
        events.where("item_id IS NOT NULL")
        .groupBy(
            "session_id",
            "item_id",
            "order_id",
            "item_name",
            "category",
            "station_id",
            "order_size",
            "sla_seconds",
        )
        .agg(
            *[
                F.min(
                    F.when(
                        F.col("event_type") == kind, F.col("event_time").cast("timestamp").cast("long")
                    )
                ).alias(column)
                for kind, column in [
                    ("PLACED", "placed_at"),
                    ("PREPARING", "started_at"),
                    ("READY", "ready_at"),
                ]
            ]
        )
    )
    end = F.coalesce("ready_at", F.lit(clock))
    prep = end - F.col("started_at")
    wait = F.coalesce("started_at", F.lit(clock)) - F.col("placed_at")
    return (
        timeline.withColumn(
            "status",
            F.when(F.col("ready_at").isNotNull(), "READY")
            .when(F.col("started_at").isNotNull(), "PREPARING")
            .otherwise("PLACED"),
        )
        .withColumn("prep_seconds", F.when(prep >= 0, prep))
        .withColumn("wait_seconds", F.when(wait >= 0, wait))
    )


def count_when(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def reports(items):
    ready = F.col("status") == "READY"
    overdue = F.col("prep_seconds") > F.col("sla_seconds")
    avg_prep = F.avg(F.when(ready, F.col("prep_seconds"))).alias("avg_prep")
    orders = items.groupBy("session_id", "order_id").agg(
        F.min("placed_at").alias("placed_at"),
        F.max("order_size").alias("item_count"),
        count_when(ready).alias("ready_count"),
    )
    orders = orders.withColumn(
        "status", F.when(F.col("ready_count") == F.col("item_count"), "READY").otherwise("OPEN")
    )
    stations = items.groupBy("session_id", "station_id").agg(
        count_when(F.col("status") == "PLACED").alias("waiting"),
        count_when(F.col("status") == "PREPARING").alias("preparing"),
        count_when(ready).alias("completed"),
        avg_prep,
        count_when(overdue).alias("violations"),
    )
    categories = items.groupBy("session_id", "category").agg(
        count_when(ready).alias("completed"), avg_prep, count_when(overdue).alias("violations")
    )
    alerts = (
        items.where(overdue)
        .withColumn("delay_seconds", F.col("prep_seconds") - F.col("sla_seconds"))
        .withColumn("alert_time", F.col("started_at") + F.col("sla_seconds") + 1)
        .withColumn("status", F.when(ready, "Resolved").otherwise("Active"))
        .select("item_id", "session_id", "status", "alert_time", "delay_seconds")
    )
    return {
        "Orders": orders,
        "OrderItems": items.drop("order_size"),
        "StationMetrics": stations,
        "CategoryMetrics": categories,
        "SLAAlerts": alerts,
    }
