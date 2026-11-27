"""Create a reproducible sample of the raw stream in two consecutive time slices."""

import json
from pathlib import Path
import random

from simulator.kitchen import advance, empty_day

if __name__ == "__main__":
    kitchen = empty_day(1767268800)  # 2026-01-01 12:00:00 UTC
    rng = random.Random(42)
    session = "30873bbe-05e5-44be-b457-23e65d02d001"
    for part in (1, 2):
        events = advance(kitchen, session, "Running", 120, 1, "Normal Traffic", 1, rng)
        if part == 2:
            events += advance(kitchen, session, "Closing", 3600, 1, "Normal Traffic", 1, rng)
        path = Path(__file__).with_name(f"part-{part}.jsonl")
        with path.open("w", encoding="utf-8", newline="\n") as out:
            for event in events:
                out.write(json.dumps(event) + "\n")
