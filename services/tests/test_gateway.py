from __future__ import annotations

import json
import logging
import re

import pytest
from fastapi.testclient import TestClient

from api_gateway.main import app
from api_gateway.models import MeasurementIn


client = TestClient(app)

VALID_MEASUREMENT = {
    "source_host": "h1",
    "target_host": "h2",
    "metric": "latency",
    "values_ms": [10, 11],
}


def assert_not_implemented(response) -> None:
    assert response.status_code == 501
    assert response.json() == {"error": "not implemented"}


def assert_bad_request(response) -> None:
    assert response.status_code == 400
    assert set(response.json()) == {"error"}


def test_health() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "api-gateway"}


def test_valid_values_ms_reaches_placeholder() -> None:
    assert_not_implemented(client.post("/measurements", json=VALID_MEASUREMENT))


def test_values_alias_reaches_placeholder() -> None:
    body = {key: value for key, value in VALID_MEASUREMENT.items() if key != "values_ms"}
    body["values"] = [10, 11]

    assert_not_implemented(client.post("/measurements", json=body))


def test_empty_and_missing_bodies_are_400() -> None:
    assert_bad_request(client.post("/measurements", json={}))
    assert_bad_request(
        client.post(
            "/measurements",
            content=b"",
            headers={"Content-Type": "application/json"},
        )
    )


def test_missing_target_host_is_400() -> None:
    body = {key: value for key, value in VALID_MEASUREMENT.items() if key != "target_host"}

    assert_bad_request(client.post("/measurements", json=body))


def test_empty_samples_are_400() -> None:
    body = {**VALID_MEASUREMENT, "values_ms": []}

    assert_bad_request(client.post("/measurements", json=body))


@pytest.mark.parametrize("invalid_value", ["10", True, float("nan"), -1])
def test_invalid_samples_are_400(invalid_value) -> None:
    body = {**VALID_MEASUREMENT, "values_ms": [invalid_value]}
    if isinstance(invalid_value, float) and invalid_value != invalid_value:
        response = client.post(
            "/measurements",
            content=json.dumps(body, allow_nan=True).encode(),
            headers={"Content-Type": "application/json"},
        )
    else:
        response = client.post("/measurements", json=body)

    assert_bad_request(response)


def test_analysis_is_required_only_for_workflows_b_and_c() -> None:
    assert_not_implemented(client.post("/measurements", json=VALID_MEASUREMENT))
    assert_bad_request(client.post("/measurements/analyze", json=VALID_MEASUREMENT))
    assert_bad_request(client.post("/measurements/m-1/analyze", json={}))


def test_invalid_thresholds_are_400() -> None:
    for threshold in (0, -1):
        body = {**VALID_MEASUREMENT, "threshold_ms": threshold}
        assert_bad_request(client.post("/measurements", json=body))


def test_invalid_packet_counts_are_400() -> None:
    for field, value in (("packets_sent", -1), ("packets_sent", True)):
        body = {**VALID_MEASUREMENT, field: value}
        assert_bad_request(client.post("/measurements", json=body))


def test_invalid_hosts_are_400() -> None:
    for host in ("h/1", "h 1"):
        body = {**VALID_MEASUREMENT, "source_host": host}
        assert_bad_request(client.post("/measurements", json=body))


def test_timestamp_defaults_to_utc_z() -> None:
    model = MeasurementIn.model_validate(VALID_MEASUREMENT)

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", model.timestamp)


def test_timestamp_offset_is_normalized_to_utc() -> None:
    model = MeasurementIn.model_validate(
        {**VALID_MEASUREMENT, "timestamp": "2026-01-01T12:00:00+02:00"}
    )

    assert model.timestamp == "2026-01-01T10:00:00Z"


def test_invalid_timestamp_is_rejected() -> None:
    response = client.post(
        "/measurements",
        json={**VALID_MEASUREMENT, "timestamp": "not-a-timestamp"},
    )

    assert_bad_request(response)


def test_invalid_json_is_400() -> None:
    response = client.post(
        "/measurements",
        content=b"{invalid",
        headers={"Content-Type": "application/json"},
    )

    assert_bad_request(response)


def test_not_found_uses_error_format() -> None:
    response = client.get("/gibts-nicht")

    assert response.status_code == 404
    assert set(response.json()) == {"error"}


def test_analyze_route_is_reached_before_path_parameter_route(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="api-gateway"):
        response = client.post("/measurements/analyze", json=VALID_MEASUREMENT)

    assert_bad_request(response)
    assert "analysis: Field required" in response.json()["error"]
    assert "POST /measurements/{measurement_id}/analyze" not in caplog.text


def test_openapi_lists_all_paths() -> None:
    paths = app.openapi()["paths"]

    assert set(paths) >= {
        "/health",
        "/measurements",
        "/measurements/analyze",
        "/measurements/{measurement_id}/analyze",
        "/jobs/{job_id}",
        "/measurements/{measurement_id}",
        "/reports/{report_id}",
    }
