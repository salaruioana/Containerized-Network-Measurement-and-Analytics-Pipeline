"""Fixtures for the public API suite.

Black box: it talks to your gateway over HTTP and knows nothing else about it.

    cd tests
    API_BASE_URL=http://localhost:8080 pytest
"""

from __future__ import annotations

import os
import time

import pytest
import requests

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8080").rstrip("/")

JOB_TIMEOUT_SECONDS = 30
POLL_INTERVAL_SECONDS = 0.25

TERMINAL_STATUSES = {
    "completed",
    "failed_measurement_storage",
    "failed_analysis",
    "failed_report_storage",
}


@pytest.fixture(scope="session")
def api() -> str:
    try:
        requests.get(f"{API_BASE_URL}/health", timeout=10).raise_for_status()
    except requests.RequestException as exc:
        pytest.fail(f"no gateway at {API_BASE_URL}: {exc}\n"
                    f"Start your stack first, or set API_BASE_URL.")
    return API_BASE_URL


@pytest.fixture
def measurement() -> dict:
    """The assignment's own example body: average 17.4 ms, one 45 ms outlier."""
    return {
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values_ms": [10, 11, 9, 45, 12],
        "unit": "ms",
        "timestamp": "2026-01-01T12:00:00Z",
    }


@pytest.fixture
def await_job(api):
    """Poll a job until it reaches a terminal status, and return it."""

    def _await(job_id: str, expected: str = "completed") -> dict:
        deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
        job = {}
        while time.monotonic() < deadline:
            response = requests.get(f"{api}/jobs/{job_id}", timeout=10)
            assert response.status_code == 200, f"job {job_id} disappeared: {response.text}"
            job = response.json()
            if job["status"] in TERMINAL_STATUSES:
                assert job["status"] == expected, f"job {job_id} ended as {job['status']}: {job}"
                return job
            time.sleep(POLL_INTERVAL_SECONDS)
        pytest.fail(f"job {job_id} never left {job.get('status')} within "
                    f"{JOB_TIMEOUT_SECONDS}s: {job}")

    return _await
