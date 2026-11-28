"""Create a reproducible sample of the raw stream in two consecutive time slices."""

import json
from pathlib import Path
import random

from simulator.kitchen import advance, empty_day

if __name__ == "__main__":
    kitchen = empty_day(1767268800)  # 2026-01-01 12:00:00 UTC
    rng = random.Random(42)
    session = "6f3e172f-b405-4d37-b44b-9ad1c38c074d"
    for part in (1, 2):
        events = advance(kitchen, session, "Running", 120, 1, "Normal Traffic", 1, rng)
        if part == 2:
            events += advance(kitchen, session, "Closing", 3600, 1, "Normal Traffic", 1, rng)
        path = Path(__file__).with_name(f"part-{part}.jsonl")
        with path.open("w", encoding="utf-8", newline="\n") as out:
            for event in events:
                out.write(json.dumps(event) + "\n")
