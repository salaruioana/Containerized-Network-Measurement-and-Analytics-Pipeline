"""Unit tests for the shared S3 helper in aic_common.s3.

See Task group-05#7.
Tests path-style addressing configuration, JSON upload and retrieval,
existence checking, error handling, and deterministic key/path helpers.
"""

from __future__ import annotations

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from aic_common.contract import AnalyticsResult, Buckets
from aic_common.s3 import (
    S3ObjectNotFoundError,
    exists,
    get_json,
    get_s3_client,
    key_from_object_path,
    measurement_key,
    measurement_object_path,
    parse_object_path,
    put_json,
    report_id_for,
    report_key,
    report_object_path,
)


@pytest.fixture(autouse=True)
def clean_aws_env(monkeypatch):
    """Ensure proxy and AWS credentials don't interfere with tests."""
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_get_s3_client_config(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", "http://test-garage:3900")
    monkeypatch.setenv("S3_REGION", "test-region")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-access")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")

    client = get_s3_client()
    assert client.meta.endpoint_url == "http://test-garage:3900"
    assert client.meta.region_name == "test-region"
    # Ensure path-style addressing and bounded timeouts are configured for Garage
    assert client.meta.config.s3.get("addressing_style") == "path"
    assert client.meta.config.connect_timeout == 5
    assert client.meta.config.read_timeout == 10
    assert client.meta.config.retries.get("total_max_attempts") == 3


def test_put_and_get_json():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.MEASUREMENTS)

        data = {"measurement_id": "m-001", "value": 42}
        key = "h1-h2/m-001.json"

        put_json(Buckets.MEASUREMENTS, key, data, client=client)

        result = get_json(Buckets.MEASUREMENTS, key, client=client)
        assert result == data


def test_get_json_not_found():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.REPORTS)

        with pytest.raises(S3ObjectNotFoundError) as exc_info:
            get_json(Buckets.REPORTS, "r-missing.json", client=client)
        assert "r-missing.json" in str(exc_info.value)


def test_exists():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.REPORTS)

        assert exists(Buckets.REPORTS, "r-001.json", client=client) is False

        put_json(Buckets.REPORTS, "r-001.json", {"status": "ok"}, client=client)
        assert exists(Buckets.REPORTS, "r-001.json", client=client) is True


def test_key_helpers():
    # Measurement keys and paths
    assert measurement_key("h1", "h2", "m-001") == "h1-h2/m-001.json"
    assert (
        measurement_object_path("h1", "h2", "m-001")
        == "measurements/h1-h2/m-001.json"
    )

    # Report keys and paths
    assert report_key("r-001") == "r-001.json"
    assert report_key("r-001.json") == "r-001.json"
    # Derives from measurement_id automatically
    assert report_key("m-001") == "r-001.json"
    assert report_object_path("r-001") == "reports/r-001.json"


def test_report_id_for():
    assert report_id_for("m-a1b2") == "r-a1b2"
    assert report_id_for("m-123456") == "r-123456"

    with pytest.raises(ValueError):
        report_id_for("invalid-prefix")

    with pytest.raises(ValueError):
        report_id_for("m-")

    with pytest.raises(ValueError):
        report_id_for(None)


def test_parse_and_key_from_object_path():
    bucket, key = parse_object_path("measurements/h1-h2/m-001.json")
    assert bucket == Buckets.MEASUREMENTS
    assert key == "h1-h2/m-001.json"

    assert (
        key_from_object_path("measurements/h1-h2/m-001.json", expected_bucket=Buckets.MEASUREMENTS)
        == "h1-h2/m-001.json"
    )
    assert key_from_object_path("/reports/r-001.json", expected_bucket=Buckets.REPORTS) == "r-001.json"

    for invalid_path in ("", "   ", "measurements", "measurements/", None):
        with pytest.raises(ValueError):
            parse_object_path(invalid_path)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Unexpected bucket"):
        key_from_object_path("reports/r-001.json", expected_bucket=Buckets.MEASUREMENTS)


def test_put_json_pydantic_legacy_dict_and_nan_rejection():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.REPORTS)

        model = AnalyticsResult(
            measurement_id="m-001",
            metric="latency",
            sample_count=3,
            average_latency_ms=12.5,
        )
        put_json(Buckets.REPORTS, "r-001.json", model, client=client)
        stored = get_json(Buckets.REPORTS, "r-001.json", client=client)
        assert stored["measurement_id"] == "m-001"
        assert stored["average_latency_ms"] == 12.5
        assert "throughput_avg_mbps" not in stored

        class LegacyDictModel:
            def dict(self, exclude_none: bool = False):
                return {"measurement_id": "m-002", "ok": True}

        put_json(Buckets.REPORTS, "r-002.json", LegacyDictModel(), client=client)
        assert get_json(Buckets.REPORTS, "r-002.json", client=client) == {
            "measurement_id": "m-002",
            "ok": True,
        }

        with pytest.raises(ValueError):
            put_json(Buckets.REPORTS, "r-nan.json", {"bad": float("nan")}, client=client)


def test_default_s3_client_and_non_404_client_errors(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.MEASUREMENTS)
        monkeypatch.setattr("aic_common.s3.get_s3_client", lambda: client)

        put_json(Buckets.MEASUREMENTS, "h1-h2/m-def.json", {"v": 1})
        assert exists(Buckets.MEASUREMENTS, "h1-h2/m-def.json") is True
        assert get_json(Buckets.MEASUREMENTS, "h1-h2/m-def.json") == {"v": 1}
        assert exists("nonexistent-bucket", "h1-h2/m-def.json") is False

    class FailingS3Client:
        def get_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "Server error"}, "ResponseMetadata": {"HTTPStatusCode": 500}},
                "GetObject",
            )

        def head_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
                "HeadObject",
            )

    failing = FailingS3Client()
    with pytest.raises(ClientError):
        get_json(Buckets.MEASUREMENTS, "k.json", client=failing)
    with pytest.raises(ClientError):
        exists(Buckets.MEASUREMENTS, "k.json", client=failing)

