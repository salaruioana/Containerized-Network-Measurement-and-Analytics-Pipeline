"""Unit tests for Measurement Storage Service (tasks #18 and #19).

Tests message parsing, validation, raw measurement Garage persistence,
manual ACK/NACK handling, idempotency, and graceful shutdown.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import boto3
import pika
import pytest
import redis
from moto import mock_aws

from aic_common.contract import Buckets, JobStatus, Queues
from aic_common.s3 import exists, get_json
from storage_service.main import (
    StorageMessage,
    StorageWriteError,
    build_raw_measurement_document,
    complete_job_workflow_a,
    connect_redis,
    handle_message,
    handle_storage_failure,
    main,
    parse_storage_message,
    process_message,
    request_analysis_workflow_b,
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

    def hget(self, name: str, key: str) -> str | None:
        return self._hashes.get(name, {}).get(key)

    def eval(self, script: str, numkeys: int, *args) -> int:
        key = args[0]
        new_status = args[1]
        allowed_previous = args[2:]
        current = self.hget(key, "status")
        if not current:
            return -1
        if current == new_status:
            return 0
        if current in allowed_previous:
            self.hset(key, "status", new_status)
            return 1
        return -2

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
    assert msg.unit is None
    assert msg.threshold_ms is None


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

    assert "unit" not in doc
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


# --- Task #22: Workflow B: mark the job stored and request analysis tests ---


def test_request_analysis_workflow_b_sets_stored_and_publishes(fake_redis):
    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.QUEUED.value},
    )
    payload = {
        "job_id": "job-002",
        "measurement_id": "m-002",
        "source_host": "h1",
        "target_host": "h3",
        "metric": "latency",
        "values_ms": [10, 11, 9, 45, 12],
        "analyze": True,
        "analysis": "latency_summary",
        "threshold_ms": 25,
    }
    msg = parse_storage_message(payload)
    mock_channel = MagicMock()

    analysis_payload = request_analysis_workflow_b(msg, fake_redis, channel=mock_channel)

    # 1. Redis status updated to stored (single field, no overwrite)
    job = fake_redis.hgetall("job:job-002")
    assert job["status"] == JobStatus.STORED.value
    assert job["measurement_id"] == "m-002"

    # 2. Queue declared durable
    mock_channel.queue_declare.assert_called_with(
        queue=Queues.ANALYSIS_REQUESTED, durable=True
    )

    # 3. Message published to exactly one queue with delivery_mode=2
    mock_channel.basic_publish.assert_called_once()
    publish_args, publish_kwargs = mock_channel.basic_publish.call_args
    assert publish_kwargs["exchange"] == ""
    assert publish_kwargs["routing_key"] == Queues.ANALYSIS_REQUESTED
    assert publish_kwargs["properties"].delivery_mode == 2
    assert publish_kwargs["properties"].content_type == "application/json"

    # 4. Message body matches {job_id, measurement_id, analysis, threshold_ms}
    published_body = json.loads(publish_kwargs["body"].decode("utf-8"))
    assert published_body == {
        "job_id": "job-002",
        "measurement_id": "m-002",
        "analysis": "latency_summary",
        "threshold_ms": 25,
    }
    assert analysis_payload == published_body


def test_process_message_workflow_b_full_sequence(mock_s3, fake_redis, valid_flat_payload):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"
    valid_flat_payload["threshold_ms"] = 35

    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.QUEUED.value},
    )

    msg = parse_storage_message(valid_flat_payload)
    mock_channel = MagicMock()

    process_message(msg, s3_client=mock_s3, redis_client=fake_redis, channel=mock_channel)

    # 1. S3 raw measurement written
    assert exists(Buckets.MEASUREMENTS, "h1-h2/m-002.json", client=mock_s3) is True
    # 2. Redis metadata written
    assert fake_redis.hgetall("meas:m-002")["status"] == "stored"
    # 3. Redis job status set to stored
    assert fake_redis.hgetall("job:job-002")["status"] == JobStatus.STORED.value
    # 4. RabbitMQ published to measurement.analysis.requested
    mock_channel.basic_publish.assert_called_once()
    _, kwargs = mock_channel.basic_publish.call_args
    assert kwargs["routing_key"] == Queues.ANALYSIS_REQUESTED


def test_handle_message_workflow_b_ack_on_success(mock_s3, fake_redis, valid_flat_payload):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"

    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.QUEUED.value},
    )

    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 88
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

    channel.basic_ack.assert_called_once_with(delivery_tag=88)
    channel.basic_nack.assert_not_called()
    assert fake_redis.hgetall("job:job-002")["status"] == JobStatus.STORED.value
    channel.basic_publish.assert_called_once()


def test_workflow_b_publish_failure_triggers_nack_requeue(
    mock_s3, fake_redis, valid_flat_payload
):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"

    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.QUEUED.value},
    )

    channel = MagicMock()
    channel.basic_publish.side_effect = pika.exceptions.AMQPError("Simulated broker disconnect")
    method = MagicMock()
    method.delivery_tag = 99
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

    # Publish failure must NOT ack, must nack with requeue=True for at-least-once delivery
    channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=True)
    channel.basic_ack.assert_not_called()


def test_workflow_b_idempotent_redelivery(mock_s3, fake_redis, valid_flat_payload):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"

    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.QUEUED.value},
    )

    msg = parse_storage_message(valid_flat_payload)
    mock_channel = MagicMock()

    # First delivery
    process_message(msg, s3_client=mock_s3, redis_client=fake_redis, channel=mock_channel)
    # Second delivery (simulated redelivery)
    process_message(msg, s3_client=mock_s3, redis_client=fake_redis, channel=mock_channel)

    # S3 still has exactly 1 object
    response = mock_s3.list_objects_v2(Bucket=Buckets.MEASUREMENTS)
    assert response["KeyCount"] == 1
    # Redis still has status stored
    assert fake_redis.hgetall("job:job-002")["status"] == JobStatus.STORED.value
    # Channel was published to twice
    assert mock_channel.basic_publish.call_count == 2


# --- Task #23: Storage failure handling (failed_measurement_storage) tests ---


def test_handle_storage_failure_sets_status_and_logs(fake_redis, caplog):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    with caplog.at_level("INFO"):
        handle_storage_failure(
            job_id="job-001",
            exc=RuntimeError("S3 connection reset"),
            redis_client=fake_redis,
        )

    # 1. Status updated to failed_measurement_storage in Redis
    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == JobStatus.FAILED_MEASUREMENT_STORAGE.value
    assert job["measurement_id"] == "m-001"

    # 2. Log line explicitly names the job_id
    assert "job-001" in caplog.text
    assert "Failed measurement storage" in caplog.text


def test_handle_storage_failure_publishes_to_measurement_failed(fake_redis):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )
    mock_channel = MagicMock()

    handle_storage_failure(
        job_id="job-001",
        exc=RuntimeError("Garage stopped"),
        redis_client=fake_redis,
        channel=mock_channel,
        measurement_id="m-001",
    )

    # Verifies optional publish to Queues.MEASUREMENT_FAILED
    mock_channel.queue_declare.assert_called_with(
        queue=Queues.MEASUREMENT_FAILED, durable=True
    )
    mock_channel.basic_publish.assert_called_once()
    _, kwargs = mock_channel.basic_publish.call_args
    assert kwargs["routing_key"] == Queues.MEASUREMENT_FAILED
    err_payload = json.loads(kwargs["body"].decode("utf-8"))
    assert err_payload["job_id"] == "job-001"
    assert err_payload["measurement_id"] == "m-001"
    assert err_payload["status"] == JobStatus.FAILED_MEASUREMENT_STORAGE.value
    assert "Garage stopped" in err_payload["error"]


def test_process_message_s3_failure_marks_job_failed(fake_redis, valid_flat_payload, caplog):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )
    msg = parse_storage_message(valid_flat_payload)

    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = RuntimeError("Garage unreachable: Connection refused")

    with caplog.at_level("INFO"):
        with pytest.raises(StorageWriteError, match="Storage write failed for job job-001"):
            process_message(msg, s3_client=mock_s3, redis_client=fake_redis)

    # 1. Job ended in failed_measurement_storage in Redis
    job = fake_redis.hgetall("job:job-001")
    assert job["status"] == JobStatus.FAILED_MEASUREMENT_STORAGE.value

    # 2. Log shows the job_id
    assert "job-001" in caplog.text
    assert "Failed measurement storage for job job-001" in caplog.text


def test_handle_message_s3_failure_acks_and_marks_failed(fake_redis, valid_flat_payload, caplog):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = RuntimeError("Garage down")

    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 123
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    with caplog.at_level("INFO"):
        handle_message(
            channel,
            method,
            properties,
            body,
            s3_client=mock_s3,
            redis_client=fake_redis,
        )

    # Acknowledges delivery because job reached terminal failure state (prevents infinite requeue)
    channel.basic_ack.assert_called_once_with(delivery_tag=123)
    channel.basic_nack.assert_not_called()

    # Redis job is marked failed_measurement_storage
    assert fake_redis.hgetall("job:job-001")["status"] == JobStatus.FAILED_MEASUREMENT_STORAGE.value
    # Log line names the job_id
    assert "job-001" in caplog.text


def test_process_message_redis_metadata_failure_marks_job_failed(mock_s3, valid_flat_payload):
    msg = parse_storage_message(valid_flat_payload)

    class FailingMetadataRedis(FakeRedis):
        def hset(self, name: str, key: str | None = None, value: Any = None, mapping: dict | None = None, **kwargs) -> int:
            if name.startswith("meas:"):
                raise RuntimeError("Redis disk full during metadata write")
            return super().hset(name, key=key, value=value, mapping=mapping, **kwargs)

    redis_client = FailingMetadataRedis()
    redis_client.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.QUEUED.value},
    )

    with pytest.raises(StorageWriteError):
        process_message(msg, s3_client=mock_s3, redis_client=redis_client)

    # Status must be set to failed_measurement_storage
    assert redis_client.hgetall("job:job-001")["status"] == JobStatus.FAILED_MEASUREMENT_STORAGE.value


# --- Task #24: Idempotent storage on redelivery tests ---


def test_redelivery_does_not_regress_analyzed_job(mock_s3, fake_redis, valid_flat_payload):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"

    # Job has already advanced to analyzed in downstream QoS service
    fake_redis.hset(
        "job:job-002",
        mapping={"measurement_id": "m-002", "status": JobStatus.ANALYZED.value},
    )

    msg = parse_storage_message(valid_flat_payload)
    mock_channel = MagicMock()

    process_message(msg, s3_client=mock_s3, redis_client=fake_redis, channel=mock_channel)

    # Must NOT regress status back to stored
    assert fake_redis.hgetall("job:job-002")["status"] == JobStatus.ANALYZED.value
    # Must NOT re-publish duplicate message to queue
    mock_channel.basic_publish.assert_not_called()


def test_redelivery_does_not_regress_completed_job(mock_s3, fake_redis, valid_flat_payload):
    valid_flat_payload["job_id"] = "job-002"
    valid_flat_payload["measurement_id"] = "m-002"
    valid_flat_payload["analyze"] = True
    valid_flat_payload["analysis"] = "latency_summary"

    # Job has already reached terminal completed state
    fake_redis.hset(
        "job:job-002",
        mapping={
            "measurement_id": "m-002",
            "status": JobStatus.COMPLETED.value,
            "report_id": "r-002",
        },
    )

    msg = parse_storage_message(valid_flat_payload)
    mock_channel = MagicMock()

    process_message(msg, s3_client=mock_s3, redis_client=fake_redis, channel=mock_channel)

    # Must NOT regress status back to stored
    job = fake_redis.hgetall("job:job-002")
    assert job["status"] == JobStatus.COMPLETED.value
    assert job["report_id"] == "r-002"
    # Must NOT re-publish duplicate message to queue
    mock_channel.basic_publish.assert_not_called()


def test_redelivery_workflow_a_already_completed(fake_redis):
    fake_redis.hset(
        "job:job-001",
        mapping={"measurement_id": "m-001", "status": JobStatus.COMPLETED.value},
    )

    complete_job_workflow_a("job-001", fake_redis)

    assert fake_redis.hgetall("job:job-001")["status"] == JobStatus.COMPLETED.value


def test_redelivery_leaves_single_s3_and_redis_artifacts(
    mock_s3, fake_redis, valid_flat_payload
):
    msg = parse_storage_message(valid_flat_payload)

    # Deliver 3 times
    for _ in range(3):
        process_message(msg, s3_client=mock_s3, redis_client=fake_redis)

    # S3 bucket has exactly 1 object
    response = mock_s3.list_objects_v2(Bucket=Buckets.MEASUREMENTS)
    assert response["KeyCount"] == 1
    assert response["Contents"][0]["Key"] == "h1-h2/m-001.json"

    # Redis metadata has exactly 1 entry for this measurement
    assert fake_redis.hgetall("meas:m-001")["status"] == "stored"


def test_drill_crash_before_ack_hook(monkeypatch, mock_s3, fake_redis, valid_flat_payload):
    monkeypatch.setenv("DRILL_CRASH_BEFORE_ACK", "job-001")
    exit_called = []

    def fake_exit(code):
        exit_called.append(code)
        raise SystemExit(code)

    monkeypatch.setattr("os._exit", fake_exit)

    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 999
    method.redelivered = False
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    # 1. First delivery crashes right before ACK
    with pytest.raises(SystemExit):
        handle_message(
            channel,
            method,
            properties,
            body,
            s3_client=mock_s3,
            redis_client=fake_redis,
        )

    assert exit_called == [42]
    # S3 and Redis writes succeeded before crash
    assert exists(Buckets.MEASUREMENTS, "h1-h2/m-001.json", client=mock_s3) is True
    assert fake_redis.hgetall("meas:m-001")["status"] == "stored"
    # Basic ACK was NOT called on channel before crash
    channel.basic_ack.assert_not_called()

    # 2. Redelivery (method.redelivered = True) restarts, finishes cleanly, and ACKs
    method.redelivered = True
    handle_message(
        channel,
        method,
        properties,
        body,
        s3_client=mock_s3,
        redis_client=fake_redis,
    )
    channel.basic_ack.assert_called_once_with(delivery_tag=999)


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


@pytest.mark.parametrize("bad_sample", [float("nan"), float("inf"), -1.0, True, "10"])
def test_invalid_samples_rejected(valid_flat_payload, bad_sample):
    valid_flat_payload["values_ms"] = [bad_sample]
    with pytest.raises(ValueError):
        parse_storage_message(valid_flat_payload)


@pytest.mark.parametrize("bad_host", ["h/1", "h 1", "..", "."])
def test_invalid_host_characters_rejected(valid_flat_payload, bad_host):
    valid_flat_payload["source_host"] = bad_host
    with pytest.raises(ValueError):
        parse_storage_message(valid_flat_payload)


def test_invalid_metric_unit_packets_and_threshold_rejected(valid_flat_payload):
    for field, bad_val in (
        ("metric", "lat\x00ency"),
        ("unit", "   "),
        ("packets_sent", -1),
        ("packets_received", True),
        ("threshold_ms", 0),
        ("threshold_ms", -5),
        ("threshold_ms", True),
        ("threshold_ms", float("inf")),
    ):
        payload = dict(valid_flat_payload)
        payload[field] = bad_val
        with pytest.raises(ValueError):
            parse_storage_message(payload)

    # JSON array instead of JSON object
    with pytest.raises(ValueError):
        parse_storage_message("[1, 2, 3]")


def test_workflow_b_omits_threshold_when_none(fake_redis):
    fake_redis.hset(
        "job:job-010",
        mapping={"measurement_id": "m-010", "status": JobStatus.QUEUED.value},
    )
    msg = parse_storage_message(
        {
            "job_id": "job-010",
            "measurement_id": "m-010",
            "source_host": "h1",
            "target_host": "h2",
            "metric": "throughput",
            "values_ms": [90, 100],
            "analysis": "throughput_summary",
        }
    )
    mock_channel = MagicMock()
    published = request_analysis_workflow_b(msg, fake_redis, channel=mock_channel)
    assert "threshold_ms" not in published
    assert published == {
        "job_id": "job-010",
        "measurement_id": "m-010",
        "analysis": "throughput_summary",
    }


def test_workflow_a_downstream_failure_guard_and_storage_failure_resilience(fake_redis):
    # Downstream terminal failure is never overwritten by a stale Workflow A redelivery
    fake_redis.hset(
        "job:job-err",
        mapping={"measurement_id": "m-err", "status": JobStatus.FAILED_ANALYSIS.value},
    )
    complete_job_workflow_a("job-err", fake_redis)
    assert fake_redis.hgetall("job:job-err")["status"] == JobStatus.FAILED_ANALYSIS.value

    # handle_storage_failure swallows secondary Redis and RabbitMQ errors safely
    broken_redis = MagicMock()
    broken_redis.hset.side_effect = RuntimeError("Redis completely unreachable")
    broken_channel = MagicMock()
    broken_channel.basic_publish.side_effect = RuntimeError("Broker closed")

    handle_storage_failure(
        job_id="job-err",
        exc=RuntimeError("Primary S3 error"),
        redis_client=broken_redis,
        channel=broken_channel,
        measurement_id="m-err",
    )


def test_connect_redis_retries_until_healthy_and_main(monkeypatch, mock_s3, fake_redis):
    created_kwargs = {}
    attempts = [0]

    class StubRedis:
        def ping(self):
            attempts[0] += 1
            if attempts[0] == 1:
                raise redis.ConnectionError("Redis starting")
            return True

    def fake_redis_cls(**kwargs):
        created_kwargs.update(kwargs)
        return StubRedis()

    monkeypatch.setattr("storage_service.main.redis.Redis", fake_redis_cls)
    client = connect_redis(host="redis-test", port=6380, retry_interval=0.001)
    assert isinstance(client, StubRedis)
    assert attempts[0] == 2
    assert created_kwargs["socket_connect_timeout"] == 5
    assert created_kwargs["socket_timeout"] == 5

    # Verify main() wires clients and starts consumer on Queues.MEASUREMENT_CREATED
    started = {}

    class StubConsumer:
        def __init__(self, **kwargs):
            started.update(kwargs)

        def start(self, setup_signals: bool = False):
            started["setup_signals"] = setup_signals

    monkeypatch.setattr("storage_service.main.get_s3_client", lambda: mock_s3)
    monkeypatch.setattr("storage_service.main.connect_redis", lambda: fake_redis)
    monkeypatch.setattr("storage_service.main.RabbitMQConsumer", StubConsumer)

    main()
    assert started["queue"] == Queues.MEASUREMENT_CREATED
    assert started["setup_signals"] is True

