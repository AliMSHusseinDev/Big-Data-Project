import json
import random
import unittest
from simulator.kitchen import advance, empty_day, STATIONS


class SimulationTests(unittest.TestCase):
    def test_speeds_and_pause(self):
        for speed in (1, 2, 5, 10):
            day = empty_day(0)
            advance(day, "s", "Running", 8, speed, "Normal Traffic", 1, random.Random(1))
            self.assertEqual(day["now"], 8 * speed)
            before = json.dumps(day)
            self.assertEqual(advance(day, "s", "Paused", 900, speed, "Normal Traffic", 1), [])
            self.assertEqual(json.dumps(day), before)
            advance(day, "s", "Running", 2, 5, "Normal Traffic", 1)
            self.assertEqual(day["now"], 8 * speed + 10)

    def test_lifecycle_capacity_and_closing(self):
        rng = random.Random(332)
        day = empty_day(1000)
        records = advance(day, "s", "Running", 160, 10, "Rush Hour", 2, rng)
        orders = day["orders"]
        self.assertGreater(orders, 5)
        self.assertTrue(any(day["waiting"].values()))
        self.assertEqual(advance(day, "s", "Paused", 100, 10, "Kitchen Delay", 1, rng), [])
        records += advance(day, "s", "Closing", 10000, 10, "Kitchen Delay", 1, rng)
        self.assertTrue(day["finished"])
        self.assertEqual(day["orders"], orders)
        self.assertEqual(records[-1]["event_type"], "FINAL")
        self.assertEqual(len({r["event_id"] for r in records}), day["events"])
        life, busy = {}, {s["id"].lower(): 0 for s in STATIONS}
        capacity = {s["id"].lower(): s["capacity"] for s in STATIONS}
        for r in records:
            if "item_id" in r:
                kind, key = r["event_type"], r["station_id"]
                life.setdefault(r["item_id"], []).append(kind)
                busy[key] += int(kind == "preparing") - int(kind == "ready")
                self.assertTrue(0 <= busy[key] <= capacity[key])
        self.assertTrue(all(kinds == ["placed", "preparing", "ready"] for kinds in life.values()))

    def test_traffic_and_delay(self):
        counts = []
        for mode in ("Low Traffic", "Normal Traffic", "Rush Hour"):
            day = empty_day(0)
            advance(day, "s", "Running", 3600, 1, mode, 1, random.Random(5))
            counts.append(day["orders"])
        self.assertLess(counts[0], counts[1])
        self.assertLess(counts[1], counts[2])
        day = empty_day(0)
        records = advance(day, "s", "Running", 1200, 1, "Kitchen Delay", 1, random.Random(42))
        self.assertTrue(
            any(
                j["due"] > day["now"] + 75 for j in day["cooking"] if j["item"]["station_id"] == "DRINKS"
            )
            or any(
                j["due"] > day["now"] + 360 for j in day["cooking"] if j["item"]["station_id"] == "GRILL"
            )
        )
        self.assertTrue(any(r["order_size"] > 1 for r in records if "order_size" in r))


if __name__ == "__main__":
    unittest.main()
