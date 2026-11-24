"""Real local Spark DataFrames, in the same container as the streaming job."""

from datetime import datetime, timezone
import unittest
from pyspark.sql import SparkSession
from spark.analytics import item_states, reports, EVENT_SCHEMA


def record(i, kind, second):
    now = datetime.fromtimestamp(second, timezone.utc).isoformat().replace("+00:00", "Z")
    return (f"{i}-{kind}", "test", kind, now, f"I{i}", "order", "Burger", "Burgers", "GRILL", 5, 360)


class AnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Kitchen analytics tests")
            .config("spark.sql.shuffle.partitions", 2)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_waiting_and_active_alerts(self):
        events = [record(i, "PLACED", 10) for i in range(5)] + [
            record(i, "PREPARING", 20) for i in range(2)
        ]
        items = item_states(self.spark.createDataFrame(events, EVENT_SCHEMA), 500)
        metrics = reports(items)
        station = metrics["StationMetrics"].first()
        self.assertEqual((station.waiting, station.preparing, station.completed), (3, 2, 0))
        alerts = metrics["SLAAlerts"].collect()
        self.assertEqual(len(alerts), 2)
        self.assertTrue(all(a.delay_seconds == 120 and a.status == "Active" for a in alerts))
        self.assertIsNone(metrics["CategoryMetrics"].first().avg_prep)

    def test_average_replay_and_resolution(self):
        events = []
        for i, duration in enumerate([360, 480, 600]):
            events.extend(
                [record(i, "READY", 50 + duration), record(i, "PLACED", 1), record(i, "PREPARING", 50)]
            )
        events += events[:3]
        metrics = reports(item_states(self.spark.createDataFrame(events, EVENT_SCHEMA), 800))
        self.assertEqual(metrics["CategoryMetrics"].first().avg_prep, 480)
        self.assertEqual(metrics["StationMetrics"].first().completed, 3)
        self.assertEqual(
            sorted((a.delay_seconds, a.status) for a in metrics["SLAAlerts"].collect()),
            [(120, "Resolved"), (240, "Resolved")],
        )
        self.assertEqual(metrics["Orders"].first().status, "OPEN")


if __name__ == "__main__":
    unittest.main()
