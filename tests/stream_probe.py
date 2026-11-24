"""Consume the wire fixture with Structured Streaming twice using the same checkpoint."""

import tempfile
from pyspark.sql import SparkSession
from spark.analytics import parse_events, item_states, reports

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("Kitchen Kafka integration")
        .config("spark.sql.shuffle.partitions", 2)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "kitchen.cleaned")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1)
        .load()
    )
    with tempfile.TemporaryDirectory() as tmp:
        sink = (
            parse_events(raw)
            .writeStream.format("parquet")
            .option("path", tmp + "/events")
            .option("checkpointLocation", tmp + "/checkpoint")
            .trigger(availableNow=True)
        )
        sink.start().awaitTermination()
        history = spark.read.parquet(tmp + "/events")
        n = history.count()
        assert n >= 4
        metrics = reports(item_states(history, 1767270000))
        for row in metrics["StationMetrics"].collect():
            assert (row.waiting, row.preparing, row.completed, row.avg_prep, row.violations) == (
                0,
                0,
                1,
                120.0,
                1,
            ), row
        sink.start().awaitTermination()
        assert spark.read.parquet(tmp + "/events").count() == n
        print(
            f"PASS: {n} Kafka messages across micro-batches, resolved SLA, 120 second prep, unchanged count after recovery."
        )
    spark.stop()
