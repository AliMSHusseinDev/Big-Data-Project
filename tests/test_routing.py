import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from dashboard.routing import check_routes


class RoutingTests(unittest.TestCase):
    def compare(self, raw, cleaned):
        records = {"kitchen.raw": raw, "kitchen.cleaned": cleaned, "kitchen.invalid": []}
        consumer = Mock()
        consumer.partitions_for_topic.return_value = {0}
        consumer.beginning_offsets.side_effect = lambda parts: dict.fromkeys(parts, 0)
        consumer.end_offsets.side_effect = lambda parts: {p: len(records[p.topic]) for p in parts}
        with patch(
            "dashboard.routing.records_between", side_effect=lambda c, p, a, b: records[p.topic][a:b]
        ):
            return check_routes(consumer, "day")

    def message(self, offset, source=None):
        return SimpleNamespace(
            offset=offset,
            key=b"day",
            value=json.dumps({"session_id": "day", "event_id": "E1"}).encode(),
            headers=[] if source is None else [("kafka.offset", str(source).encode())],
        )

    def test_older_day_with_one_extra_cleaned_copy(self):
        totals, differences = self.compare([self.message(0)], [self.message(0), self.message(1)])
        self.assertEqual(totals, [1, 2, 0])
        self.assertEqual(
            differences,
            [{"Event ID or payload": "E1", "Raw offset": None, "Raw": 1, "Cleaned": 2, "Invalid": 0}],
        )

    def test_duplicate_and_missing_raw_offsets_with_equal_totals(self):
        totals, differences = self.compare(
            [self.message(0), self.message(1)], [self.message(0, 0), self.message(1, 0)]
        )
        self.assertEqual(totals, [2, 2, 0])
        self.assertEqual({row["Raw offset"]: row["Cleaned"] for row in differences}, {0: 2, 1: 0})
