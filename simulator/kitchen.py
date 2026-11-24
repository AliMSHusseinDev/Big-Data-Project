"""A discrete-event kitchen. Every time in this module is measured in simulated seconds."""

from datetime import datetime, timezone
import json
from pathlib import Path
import random

CONFIG = json.loads((Path(__file__).resolve().parents[1] / "config/restaurant.json").read_text())
STATIONS = CONFIG["stations"]


def empty_day(opened):
    return dict(
        now=float(opened),
        next_order=opened,
        next_clock=opened,
        orders=0,
        items=0,
        events=0,
        finished=False,
        waiting={s["id"]: [] for s in STATIONS},
        cooking=[],
    )


def event(day, session, kind, now, item=None):
    day["events"] += 1
    record = dict(
        event_id=f"{session}-E{day['events']}",
        session_id=session,
        event_type=kind,
        event_time=datetime.fromtimestamp(now, timezone.utc).isoformat(),
    )
    if item:
        record.update(item)
        # Controlled, valid inconsistencies make the cleaning step visible.
        record["item_name"] = " " + item["item_name"].lower() + " "
        record["category"] = item["category"].lower()
        record["station_id"] = item["station_id"].lower()
        record["event_type"] = kind.lower()
        record["sla_seconds"] = str(item["sla_seconds"])
    return record


def place_order(day, session, now, rng):
    day["orders"] += 1
    size = rng.randint(1, 5)
    records = []
    for _ in range(size):
        station = rng.choice(STATIONS)
        day["items"] += 1
        item = dict(
            item_id=f"{session}-I{day['items']}",
            order_id=f"{session}-O{day['orders']}",
            order_size=size,
            item_name=rng.choice(station["menu"]),
            category=station["category"],
            station_id=station["id"],
            sla_seconds=station["sla_seconds"],
        )
        day["waiting"][station["id"]].append(item)
        records.append(event(day, session, "PLACED", now, item))
    return records


def serve_stations(day, session, now, scenario, rng):
    records = []
    for job in day["cooking"][:]:
        if job["due"] <= now:
            records.append(event(day, session, "READY", job["due"], job["item"]))
            day["cooking"].remove(job)
    for station in STATIONS:
        key = station["id"]
        busy = sum(job["item"]["station_id"] == key for job in day["cooking"])
        queue = day["waiting"][key]
        while queue and busy < station["capacity"]:
            item = queue.pop(0)
            duration = rng.randint(*station["prep_seconds"])
            if scenario == "Kitchen Delay" and key in ("GRILL", "DRINKS"):
                duration += station["sla_seconds"]
            day["cooking"].append(dict(item=item, due=now + duration))
            records.append(event(day, session, "PREPARING", now, item))
            busy += 1
    return records


def advance(day, session, status, elapsed, speed, scenario, factor, rng=random):
    if status not in ("Running", "Closing") or day["finished"]:
        return []
    target = day["now"] + elapsed * speed
    records = []
    # Jump between arrivals, completions and clock ticks; no per-second busy loop.
    while True:
        times = [day["next_clock"]] + [job["due"] for job in day["cooking"]]
        if status == "Running":
            times.append(day["next_order"])
        now = min(times)
        if now > target:
            break
        if status == "Running" and now == day["next_order"]:
            records.extend(place_order(day, session, now, rng))
            interval = rng.randint(*CONFIG["arrival_seconds"][scenario])
            day["next_order"] = now + max(1, round(interval / factor))
        records.extend(serve_stations(day, session, now, scenario, rng))
        if status == "Closing" and not day["cooking"] and not any(day["waiting"].values()):
            records.append(event(day, session, "FINAL", now))
            day.update(now=float(now), finished=True)
            return records
        if now == day["next_clock"]:
            records.append(event(day, session, "CLOCK", now))
            day["next_clock"] += 5
    day["now"] = target
    return records
