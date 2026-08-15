#!/usr/bin/env python3
from __future__ import annotations

import json
import sys


containers = []
for item in json.load(sys.stdin):
    containers.append({
        "Id": item["Id"],
        "Image": item["Image"],
        "Mounts": sorted(({
            "Destination": value.get("Destination"),
            "Mode": value.get("Mode"),
            "Name": value.get("Name"),
            "RW": value.get("RW"),
            "Source": value.get("Source"),
            "Type": value.get("Type"),
        } for value in item.get("Mounts", [])), key=lambda value: value["Destination"] or ""),
        "Name": item["Name"],
        "Networks": {
            name: {
                "EndpointID": value.get("EndpointID"),
                "IPAddress": value.get("IPAddress"),
                "NetworkID": value.get("NetworkID"),
            }
            for name, value in sorted(item["NetworkSettings"]["Networks"].items())
        },
        "RestartCount": item["RestartCount"],
    })
json.dump(sorted(containers, key=lambda value: value["Id"]), sys.stdout,
          sort_keys=True, separators=(",", ":"))
print()
