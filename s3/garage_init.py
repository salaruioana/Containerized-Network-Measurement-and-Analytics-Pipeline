"""One-shot Garage setup: create the buckets and give the shared S3 key access.

Garage runs with `--single-node --default-access-key`, so it applies its own
layout and imports S3_ACCESS_KEY / S3_SECRET_KEY on startup. This script only
adds the buckets. It is idempotent and safe to run on every start.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

ADMIN_URL = os.environ.get("GARAGE_ADMIN_URL", "http://garage:3903")
TOKEN = os.environ["GARAGE_ADMIN_TOKEN"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
# Names fixed by CONTRACT.md §4; `errors` is the optional one.
BUCKETS = ("measurements", "reports", "errors")


def call(method: str, path: str, body: dict | None = None):
    request = urllib.request.Request(
        ADMIN_URL + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_until_healthy(timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            urllib.request.urlopen(ADMIN_URL + "/health", timeout=5)
            return
        except OSError as exc:
            if time.monotonic() > deadline:
                sys.exit(f"garage not healthy at {ADMIN_URL}: {exc}")
            time.sleep(2)


def main() -> None:
    wait_until_healthy()
    existing = {alias: b["id"] for b in call("GET", "/v2/ListBuckets") for alias in b["globalAliases"]}
    for name in BUCKETS:
        bucket_id = existing.get(name) or call("POST", "/v2/CreateBucket", {"globalAlias": name})["id"]
        call("POST", "/v2/AllowBucketKey", {
            "bucketId": bucket_id,
            "accessKeyId": ACCESS_KEY,
            "permissions": {"read": True, "write": True, "owner": True},
        })
        print(f"bucket {name} ready", flush=True)


if __name__ == "__main__":
    main()
