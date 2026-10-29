#!/usr/bin/env python3
"""Simulated network hosts. Generates ping latencies and sends them to the API.

In each round, each source host in simulator.yaml pings each of its targets.
Each result goes to the API as one latency measurement.

    pip install -r scripts/requirements.txt
    python3 scripts/measurement_simulator.py
    python3 scripts/measurement_simulator.py --config my.yaml --api http://127.0.0.1:8090

Set `host_header` in simulator.yaml when an ingress routes by hostname, for
example Kourier in front of a Knative Service. Such an ingress answers 404
without that header, even when the service is healthy. Leave it empty otherwise.

Press Ctrl-C to stop. A failed request prints an error and the run continues.
Use this to see how your stack behaves when a service is down.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).with_name("simulator.yaml")


def load_config(path: Path) -> dict:
    with path.open() as handle:
        config = yaml.safe_load(handle) or {}

    for key in ("api", "hosts", "latency_ms"):
        if key not in config:
            sys.exit(f"{path}: missing '{key}'")
    config.setdefault("rounds", 10)
    config.setdefault("interval_s", 5)
    config.setdefault("samples", 10)
    config.setdefault("analyze", False)
    config.setdefault("host_header", "")

    low, high = config["latency_ms"].get("min"), config["latency_ms"].get("max")
    if low is None or high is None or not 0 < low <= high:
        sys.exit(f"{path}: latency_ms needs 0 < min <= max, got min={low} max={high}")
    if config["samples"] < 1:
        sys.exit(f"{path}: samples must be at least 1")
    return config


def links(hosts: dict) -> list[tuple[str, str]]:
    """Return each (source, target) pair in file order."""
    return [(source, target) for source, targets in hosts.items() for target in targets or []]


def ping(samples: int, low: float, high: float) -> list[float]:
    return [round(random.uniform(low, high), 1) for _ in range(samples)]


def submit(api: str, path: str, measurement: dict, host_header: str | None) -> tuple[int, dict]:
    request = urllib.request.Request(
        f"{api.rstrip('/')}{path}",
        data=json.dumps(measurement).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if host_header:
        # add_unredirected_header, because a header set with add_header can be
        # dropped on a redirect, and a hostname-routing ingress needs it.
        request.add_unredirected_header("Host", host_header)
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status, json.loads(response.read())


def run_round(config: dict, api: str) -> None:
    path = "/measurements/analyze" if config["analyze"] else "/measurements"
    low, high = config["latency_ms"]["min"], config["latency_ms"]["max"]

    for source, target in links(config["hosts"]):
        values = ping(config["samples"], low, high)
        measurement = {
            "source_host": source,
            "target_host": target,
            "metric": "latency",
            "values_ms": values,
            "unit": "ms",
            "packets_sent": len(values),
            "packets_received": len(values),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if config["analyze"]:
            measurement["analysis"] = "latency_summary"

        try:
            status, body = submit(api, path, measurement, config["host_header"])
            print(f"  {source} -> {target}: HTTP {status} job_id={body.get('job_id')} "
                  f"measurement_id={body.get('measurement_id')}", flush=True)
        except urllib.error.HTTPError as exc:
            print(f"  {source} -> {target}: HTTP {exc.code} {exc.read().decode()}",
                  file=sys.stderr, flush=True)
        except urllib.error.URLError as exc:
            print(f"  {source} -> {target}: request failed: {exc.reason}",
                  file=sys.stderr, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                        help=f"YAML config (default: {DEFAULT_CONFIG.name} next to this script)")
    parser.add_argument("--api", help="API base URL, overrides 'api' in the config")
    args = parser.parse_args()

    config = load_config(args.config)
    api = args.api or config["api"]
    rounds = config["rounds"]

    if not links(config["hosts"]):
        sys.exit(f"{args.config}: 'hosts' lists no targets")

    round_number = 0
    try:
        while rounds == 0 or round_number < rounds:
            round_number += 1
            total = rounds or "inf"
            print(f"[{round_number}/{total}] {api}", flush=True)
            run_round(config, api)
            if rounds == 0 or round_number < rounds:
                time.sleep(config["interval_s"])
    except KeyboardInterrupt:
        print(f"\nstopped after {round_number} round(s)")


if __name__ == "__main__":
    main()
