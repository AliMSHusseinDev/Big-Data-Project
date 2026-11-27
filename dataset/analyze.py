"""Classroom exercise: read both raw time slices and count completed items by category."""

from pyspark.sql import SparkSession, functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Kitchen dataset partitions").getOrCreate()
    raw = spark.read.json("/app/dataset/part-*.jsonl")
    # Narrow: each row can be filtered and normalized in its original partition.
    completed = raw.where(F.upper("event_type") == "READY").select(
        F.initcap(F.trim("category")).alias("category")
    )
    # Wide: rows sharing a category must meet in the same partition.
    counts = completed.groupBy("category").count()
    counts.explain(mode="formatted")
    counts.show()
    spark.stop()
