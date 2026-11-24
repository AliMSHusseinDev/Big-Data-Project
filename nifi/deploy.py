"""Build the visible flow using NiFi 1.28's revision-aware REST API."""

import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).parent
BASE = os.getenv("NIFI_API", "http://nifi:8081/nifi-api")
NEW = {"version": 0}


def api(path, body=None, method=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(
        BASE + "/" + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urlopen(req, timeout=20) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"{error.code} {path}: {error.read().decode()[:1200]}") from error


def deploy():
    group = api("flow/process-groups/root")["processGroupFlow"]["id"]
    path = "process-groups/" + group
    listing = api("flow/" + path + "/controller-services")["controllerServices"]
    services = {e["component"]["name"]: e for e in listing}
    schema = {"type": "record", "name": "KitchenEvent", "fields": []}
    for key in ["event_id", "session_id", "event_type", "event_time"]:
        schema["fields"].append({"name": key, "type": "string"})
    for key in [
        "order_id",
        "item_id",
        "item_name",
        "station_id",
        "category",
        "sla_seconds",
        "order_size",
    ]:
        dtype = "int" if key in ("sla_seconds", "order_size") else "string"
        schema["fields"].append({"name": key, "type": ["null", dtype], "default": None})
    for name, implementation, props in [
        (
            "Kitchen JSON reader",
            "JsonTreeReader",
            {"schema-access-strategy": "schema-text-property", "schema-text": json.dumps(schema)},
        ),
        ("Kitchen JSON writer", "JsonRecordSetWriter", {"output-grouping": "output-oneline"}),
    ]:
        if name not in services:
            services[name] = api(
                path + "/controller-services",
                {
                    "revision": NEW,
                    "component": {
                        "type": "org.apache.nifi.json." + implementation,
                        "name": name,
                        "properties": props,
                    },
                },
            )
        entity = services[name]
        endpoint = "controller-services/" + entity["id"]
        if any(entity["component"]["properties"].get(k) != v for k, v in props.items()):
            if entity["component"]["state"] != "DISABLED":
                api(
                    endpoint + "/run-status",
                    {"revision": entity["revision"], "state": "DISABLED"},
                    "PUT",
                )
                for _ in range(30):
                    entity = api(endpoint)
                    if entity["component"]["state"] == "DISABLED":
                        break
                    time.sleep(0.5)
            entity = api(
                endpoint,
                {"revision": entity["revision"], "component": {"id": entity["id"], "properties": props}},
                "PUT",
            )
        if entity["component"]["state"] != "ENABLED":
            api(endpoint + "/run-status", {"revision": entity["revision"], "state": "ENABLED"}, "PUT")
        for _ in range(30):
            if api(endpoint)["component"]["state"] == "ENABLED":
                break
            time.sleep(1)
        else:
            raise RuntimeError(name + " did not enable")

    kafka = {"bootstrap.servers": "kafka:29092", "security.protocol": "PLAINTEXT"}
    publish = dict(kafka, **{"acks": "all", "use-transactions": "false", "kafka-key": "${message.key}"})
    prefix = "org.apache.nifi.processors."
    definitions = [
        (
            "Read raw orders",
            "kafka.pubsub.ConsumeKafka_2_6",
            0,
            0,
            dict(
                kafka,
                topic="kitchen.raw",
                **{
                    "group.id": "kitchen-etl",
                    "auto.offset.reset": "earliest",
                    "key-attribute-encoding": "utf-8",
                },
            ),
            [],
        ),
        (
            "Normalize and check menu",
            "script.ExecuteScript",
            0,
            250,
            {"Script Engine": "Groovy", "Script Body": (ROOT / "clean.groovy").read_text()},
            [],
        ),
        (
            "Validate event schema",
            "standard.ValidateRecord",
            0,
            500,
            {
                "record-reader": services["Kitchen JSON reader"]["id"],
                "record-writer": services["Kitchen JSON writer"]["id"],
                "schema-access-strategy": "reader-schema",
            },
            [],
        ),
        (
            "Publish cleaned",
            "kafka.pubsub.PublishKafka_2_6",
            0,
            800,
            dict(publish, topic="kitchen.cleaned"),
            ["success"],
        ),
        (
            "Publish rejected",
            "kafka.pubsub.PublishKafka_2_6",
            700,
            500,
            dict(publish, topic="kitchen.invalid", **{"attribute-name-regex": "record.error.message"}),
            ["success"],
        ),
    ]
    processors = {p["component"]["name"]: p for p in api(path + "/processors")["processors"]}
    for name, kind, x, y, props, terminate in definitions:
        if name not in processors:
            processors[name] = api(
                path + "/processors",
                {
                    "revision": NEW,
                    "component": {
                        "name": name,
                        "type": prefix + kind,
                        "position": {"x": x, "y": y},
                        "config": {
                            "properties": props,
                            "autoTerminatedRelationships": terminate,
                            "schedulingPeriod": "100 ms",
                            "concurrentlySchedulableTaskCount": 1,
                        },
                    },
                },
            )
        else:
            entity = processors[name]
            endpoint = "processors/" + entity["id"]
            actual = entity["component"]["config"]["properties"]
            if any(actual.get(key) != value for key, value in props.items()):
                if entity["component"]["state"] == "RUNNING":
                    api(
                        endpoint + "/run-status",
                        {"revision": entity["revision"], "state": "STOPPED"},
                        "PUT",
                    )
                    for _ in range(40):
                        entity = api(endpoint)
                        if (
                            entity["component"]["state"] == "STOPPED"
                            and entity["status"]["aggregateSnapshot"]["activeThreadCount"] == 0
                        ):
                            break
                        time.sleep(0.5)
                api(
                    endpoint,
                    {
                        "revision": entity["revision"],
                        "component": {"id": entity["id"], "config": {"properties": props}},
                    },
                    "PUT",
                )
    nodes = [processors[row[0]]["id"] for row in definitions]
    existing = set()
    for entity in api(path + "/connections")["connections"]:
        c = entity["component"]
        existing.update(
            (c["source"]["id"], r, c["destination"]["id"]) for r in c["selectedRelationships"]
        )
    for source, rels, dest in [
        (0, ["success"], 1),
        (1, ["success"], 2),
        (1, ["failure"], 4),
        (2, ["valid"], 3),
        (2, ["invalid", "failure"], 4),
        (3, ["failure"], 3),
        (4, ["failure"], 4),
    ]:
        if all((nodes[source], r, nodes[dest]) in existing for r in rels):
            continue
        component = {
            "source": {"id": nodes[source], "type": "PROCESSOR", "groupId": group},
            "destination": {"id": nodes[dest], "type": "PROCESSOR", "groupId": group},
            "selectedRelationships": rels,
        }
        if source == dest:
            x, y = definitions[source][2:4]
            component["bends"] = [{"x": x + 430, "y": y - 40}, {"x": x + 430, "y": y + 120}]
        api(path + "/connections", {"revision": NEW, "component": component})
    for node in reversed(nodes):
        endpoint = "processors/" + node
        for _ in range(40):
            entity = api(endpoint)
            if entity["component"]["validationStatus"] == "VALID":
                break
            time.sleep(0.5)
        else:
            raise RuntimeError(str(entity["component"].get("validationErrors")))
        if entity["component"]["state"] != "RUNNING":
            api(endpoint + "/run-status", {"revision": entity["revision"], "state": "RUNNING"}, "PUT")
    if any(api("processors/" + node)["component"]["state"] != "RUNNING" for node in nodes):
        raise RuntimeError("Flow did not start")
    print(f"Running: {len(nodes)} processors, 2 controller services")


if __name__ == "__main__":
    deploy()
