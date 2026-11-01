"""Unit tests for the shared S3 helper in aic_common.s3.

See Task group-05#7.
Tests path-style addressing configuration, JSON upload and retrieval,
existence checking, error handling, and deterministic key/path helpers.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from aic_common.contract import Buckets
from aic_common.s3 import (
    S3ObjectNotFoundError,
    exists,
    get_json,
    get_s3_client,
    measurement_key,
    measurement_object_path,
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
    # Ensure path-style addressing is explicitly configured for Garage compatibility
    assert client.meta.config.s3.get("addressing_style") == "path"


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
