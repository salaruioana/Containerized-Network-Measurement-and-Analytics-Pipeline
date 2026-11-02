"""Unit tests for Measurement Storage Service (tasks #18 and #19).

Tests message parsing, validation, raw measurement Garage persistence,
manual ACK/NACK handling, idempotency, and graceful shutdown.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from aic_common.contract import Buckets, JobStatus, Queues
from aic_common.s3 import exists, get_json
from storage_service.main import (
    ConsumerContext,
    StorageMessage,
    build_raw_measurement_document,
    complete_job_workflow_a,
    handle_message,
    parse_storage_message,
    process_message,
    store_measurement_metadata,
    store_raw_measurement,
)


@pytest.fixture(autouse=True)
def clean_aws_env(monkeypatch):
    """Ensure AWS credentials and proxy don't interfere with tests."""
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.MEASUREMENTS)
        yield client


class FakeRedis:
    """In-memory Redis fake for metadata hash testing."""

    def __init__(self) -> None:
        self._hashes: dict[str, dict[str, str]] = {}

    def hset(
        self,
        name: str,
        key: str | None = None,
        value: Any = None,
        mapping: dict[str, Any] | None = None,
        **kwargs,
    ) -> int:
        if name not in self._hashes:
            self._hashes[name] = {}
        merged: dict[str, str] = {}
        if key is not None and value is not None:
            merged[str(key)] = str(value)
        if mapping:
            merged.update({str(k): str(v) for k, v in mapping.items()})
        if kwargs:
            merged.update({str(k): str(v) for k, v in kwargs.items()})
        self._hashes[name].update(merged)
        return len(merged)

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self._hashes.get(name, {}))

    def ping(self) -> bool:
        return True


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def valid_flat_payload() -> dict:
    return {
        "job_id": "job-001",
        "measurement_id": "m-001",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values_ms": [10.5, 11.2, 9.8, 45.0],
        "unit": "ms",
        "timestamp": "2026-06-22T10:00:00Z",
        "packets_sent": 5,
        "packets_received": 5,
        "analyze": False,
        "threshold_ms": 30,
    }


def test_parse_valid_flat_message(valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    assert msg.job_id == "job-001"
    assert msg.measurement_id == "m-001"
    assert msg.source_host == "h1"
    assert msg.target_host == "h2"
    assert msg.metric == "latency"
    assert msg.values_ms == [10.5, 11.2, 9.8, 45.0]
    assert msg.analyze is False
    assert msg.threshold_ms == 30


def test_parse_valid_json_string(valid_flat_payload):
    payload_str = json.dumps(valid_flat_payload)
    msg = parse_storage_message(payload_str)
    assert msg.job_id == "job-001"
    assert msg.measurement_id == "m-001"


def test_parse_nested_measurement_payload():
    nested_payload = {
        "job_id": "job-002",
        "measurement_id": "m-002",
        "analyze": True,
        "analysis": "latency_summary",
        "measurement": {
            "source_host": "node-a",
            "target_host": "node-b",
            "metric": "latency",
            "values_ms": [12.0, 15.0],
        },
    }
    msg = parse_storage_message(nested_payload)
    assert msg.job_id == "job-002"
    assert msg.source_host == "node-a"
    assert msg.target_host == "node-b"
    assert msg.analyze is True
    assert msg.analysis == "latency_summary"


def test_parse_values_alias():
    payload = {
        "job_id": "job-003",
        "measurement_id": "m-003",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values": [20, 25, 30],
    }
    msg = parse_storage_message(payload)
    assert msg.values_ms == [20.0, 25.0, 30.0]
    assert msg.timestamp is not None
    assert msg.threshold_ms == 30


def test_parse_analysis_infers_analyze_flag():
    payload = {
        "job_id": "job-004",
        "measurement_id": "m-004",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values_ms": [10.0],
        "analysis": "latency_summary",
    }
    msg = parse_storage_message(payload)
    assert msg.analyze is True
    assert msg.analysis == "latency_summary"


@pytest.mark.parametrize("invalid_job_id", ["", "invalid-id", "job-", 12345, None])
def test_invalid_job_id(valid_flat_payload, invalid_job_id):
    valid_flat_payload["job_id"] = invalid_job_id
    with pytest.raises(ValueError):
        parse_storage_message(valid_flat_payload)


@pytest.mark.parametrize("invalid_meas_id", ["", "meas-001", "m-", None])
def test_invalid_measurement_id(valid_flat_payload, invalid_meas_id):
    valid_flat_payload["measurement_id"] = invalid_meas_id
    with pytest.raises(ValueError):
        parse_storage_message(valid_flat_payload)


def test_empty_values_rejected(valid_flat_payload):
    valid_flat_payload["values_ms"] = []
    with pytest.raises(ValueError, match="at least 1 sample"):
        parse_storage_message(valid_flat_payload)


def test_missing_host_rejected(valid_flat_payload):
    valid_flat_payload["source_host"] = ""
    with pytest.raises(ValueError):
        parse_storage_message(valid_flat_payload)


def test_non_dict_non_json_rejected():
    with pytest.raises(TypeError):
        parse_storage_message(12345)


# --- Task #19: Raw measurement persistence tests ---


def test_build_raw_measurement_document_with_packets(valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    doc = build_raw_measurement_document(msg)

    assert doc == {
        "measurement_id": "m-001",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values_ms": [10.5, 11.2, 9.8, 45.0],
        "unit": "ms",
        "timestamp": "2026-06-22T10:00:00Z",
        "packets_sent": 5,
        "packets_received": 5,
    }


def test_build_raw_measurement_document_without_packets():
    payload = {
        "job_id": "job-005",
        "measurement_id": "m-005",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "values_ms": [10.0],
        "timestamp": "2026-06-22T10:00:00Z",
    }
    msg = parse_storage_message(payload)
    doc = build_raw_measurement_document(msg)

    assert "packets_sent" not in doc
    assert "packets_received" not in doc
    assert doc["measurement_id"] == "m-005"


def test_store_raw_measurement_success(mock_s3, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    key = store_raw_measurement(msg, s3_client=mock_s3)

    assert key == "h1-h2/m-001.json"
    assert exists(Buckets.MEASUREMENTS, key, client=mock_s3) is True

    stored = get_json(Buckets.MEASUREMENTS, key, client=mock_s3)
    assert stored["measurement_id"] == "m-001"
    assert stored["source_host"] == "h1"
    assert stored["target_host"] == "h2"
    assert stored["metric"] == "latency"
    assert stored["values_ms"] == [10.5, 11.2, 9.8, 45.0]
    assert stored["packets_sent"] == 5
    assert stored["packets_received"] == 5


def test_store_raw_measurement_idempotent_redelivery(mock_s3, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)

    # First write
    key1 = store_raw_measurement(msg, s3_client=mock_s3)
    # Second write (simulated redelivery)
    key2 = store_raw_measurement(msg, s3_client=mock_s3)

    assert key1 == key2 == "h1-h2/m-001.json"

    # Verify bucket has only 1 object
    response = mock_s3.list_objects_v2(Bucket=Buckets.MEASUREMENTS)
    assert response["KeyCount"] == 1
    assert response["Contents"][0]["Key"] == "h1-h2/m-001.json"


def test_process_message_stores_raw_measurement(mock_s3, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    process_message(msg, s3_client=mock_s3)

    expected_key = "h1-h2/m-001.json"
    assert exists(Buckets.MEASUREMENTS, expected_key, client=mock_s3) is True


# --- Task #20: Measurement metadata in Redis tests ---


def test_store_measurement_metadata_success(fake_redis, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    meta = store_measurement_metadata(msg, redis_client=fake_redis)

    expected = {
        "measurement_id": "m-001",
        "source_host": "h1",
        "target_host": "h2",
        "metric": "latency",
        "timestamp": "2026-06-22T10:00:00Z",
        "object_path": "measurements/h1-h2/m-001.json",
        "status": "stored",
    }
    assert meta == expected
    assert fake_redis.hgetall("meas:m-001") == expected


def test_store_measurement_metadata_idempotent_redelivery(fake_redis, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)

    # First write
    store_measurement_metadata(msg, redis_client=fake_redis)
    # Second write (simulated redelivery)
    store_measurement_metadata(msg, redis_client=fake_redis)

    stored = fake_redis.hgetall("meas:m-001")
    assert len(stored) == 7
    assert stored["measurement_id"] == "m-001"
    assert stored["status"] == "stored"


def test_process_message_stores_s3_and_redis(mock_s3, fake_redis, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)
    process_message(msg, s3_client=mock_s3, redis_client=fake_redis)

    # Verify S3
    assert exists(Buckets.MEASUREMENTS, "h1-h2/m-001.json", client=mock_s3) is True

    # Verify Redis
    redis_meta = fake_redis.hgetall("meas:m-001")
    assert redis_meta["measurement_id"] == "m-001"
    assert redis_meta["object_path"] == "measurements/h1-h2/m-001.json"
    assert redis_meta["status"] == "stored"

    # Verify Job is marked completed for Workflow A
    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == "completed"


# --- Task #21: Workflow A: mark the job completed tests ---


def test_complete_job_workflow_a_single_field_update(fake_redis):
    # Seed job created by Gateway in queued state with measurement_id
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    complete_job_workflow_a("job-001", fake_redis)

    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == JobStatus.COMPLETED.value
    # Confirm single-field update: measurement_id preserved, report_id not added
    assert job["measurement_id"] == "m-001"
    assert "report_id" not in job


def test_process_message_workflow_a_completes_job(mock_s3, fake_redis, valid_flat_payload):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )
    msg = parse_storage_message(valid_flat_payload)
    assert msg.analyze is False

    process_message(msg, s3_client=mock_s3, redis_client=fake_redis)

    # 1. S3 raw measurement written
    assert exists(Buckets.MEASUREMENTS, "h1-h2/m-001.json", client=mock_s3) is True
    # 2. Redis metadata written
    assert fake_redis.hgetall("meas:m-001")["status"] == "stored"
    # 3. Redis job status set to completed
    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == JobStatus.COMPLETED.value
    assert job["measurement_id"] == "m-001"


def test_process_message_workflow_b_does_not_mark_completed(
    mock_s3, fake_redis, valid_flat_payload
):
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"
    msg = parse_storage_message(valid_flat_payload)
    assert msg.analyze is True

    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    process_message(msg, s3_client=mock_s3, redis_client=fake_redis)

    # Storage writes S3 and metadata, but does NOT mark completed (Workflow B is queued/stored)
    job = fake_redis.hgetall("job:job-001")
    assert job.get("status") != JobStatus.COMPLETED.value


def test_complete_job_workflow_a_idempotent_redelivery(fake_redis):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    # First delivery
    complete_job_workflow_a("job-001", fake_redis)
    assert fake_redis.hgetall("job:job-001")["status"] == JobStatus.COMPLETED.value

    # Second delivery (redelivery)
    complete_job_workflow_a("job-001", fake_redis)
    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == JobStatus.COMPLETED.value
    assert job["measurement_id"] == "m-001"


def test_handle_message_workflow_a_completes_job_and_acks(
    mock_s3, fake_redis, valid_flat_payload
):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 77
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    handle_message(
        channel,
        method,
        properties,
        body,
        s3_client=mock_s3,
        redis_client=fake_redis,
    )

    channel.basic_ack.assert_called_once_with(delivery_tag=77)
    channel.basic_nack.assert_not_called()
    assert fake_redis.hgetall("job:job-001")["status"] == JobStatus.COMPLETED.value


# --- Consumer callback & lifecycle tests ---


def test_handle_message_ack_on_success(mock_s3, fake_redis, valid_flat_payload):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 42
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    handle_message(
        channel,
        method,
        properties,
        body,
        s3_client=mock_s3,
        redis_client=fake_redis,
    )

    channel.basic_ack.assert_called_once_with(delivery_tag=42)
    channel.basic_nack.assert_not_called()
    assert exists(Buckets.MEASUREMENTS, "h1-h2/m-001.json", client=mock_s3) is True
    assert fake_redis.hgetall("meas:m-001")["status"] == "stored"


def test_handle_message_nack_no_requeue_on_malformed_json(mock_s3):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 99
    properties = MagicMock()
    body = b"not a json object {"

    handle_message(channel, method, properties, body, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_nack_no_requeue_on_schema_violation(mock_s3, valid_flat_payload):
    valid_flat_payload["job_id"] = "bad-prefix"
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 100
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    handle_message(channel, method, properties, body, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=100, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_nack_requeue_on_unexpected_error(monkeypatch, mock_s3, valid_flat_payload):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 101
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    def raise_crash(msg, **kwargs):
        raise RuntimeError("Unexpected simulated system crash")

    monkeypatch.setattr("storage_service.main.process_message", raise_crash)

    handle_message(channel, method, properties, body, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=101, requeue=True)
    channel.basic_ack.assert_not_called()


def test_consumer_context_stop():
    context = ConsumerContext()
    mock_channel = MagicMock()
    mock_channel.is_open = True
    context.channel = mock_channel

    assert context.stop_requested is False
    context.request_stop()
    assert context.stop_requested is True
    mock_channel.stop_consuming.assert_called_once()
