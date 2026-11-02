from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from aic_common.contract import Buckets, JobStatus
from aic_common.s3 import report_key
from report_service import main as report_main
from report_service.main import (
    ReportStorageError,
    build_report,
    handle_message,
    process_message,
    report_id_for,
)


class FakeRedis:
    """Redis fake supporting hash dict storage and updates list recording."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.updates: list[dict] = []

    def hset(self, name: str, mapping: dict | None = None, **kwargs) -> int:
        if name not in self.hashes:
            self.hashes[name] = {}
        if mapping:
            self.hashes[name].update({str(k): str(v) for k, v in mapping.items()})
            self.updates.append({"name": name, "mapping": mapping})
        return len(self.hashes[name])

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))


@pytest.fixture
def redis_client() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def fake_redis(redis_client: FakeRedis) -> FakeRedis:
    return redis_client


@pytest.fixture
def s3_client() -> Any:
    return MagicMock()


@pytest.fixture
def valid_payload() -> dict:
    return {
        "job_id": "job-123",
        "measurement_id": "m-a1b2",
        "metric": "latency",
        "sample_count": 2,
        "average_latency_ms": 15.0,
    }


# ==============================================================================
# Report ID and report construction
# ==============================================================================


def test_report_id_for():
    assert report_id_for("m-a1b2") == "r-a1b2"


def test_report_id_for_invalid_prefix():
    with pytest.raises(ValueError):
        report_id_for("x-a1b2")


def test_report_id_for_empty_suffix():
    with pytest.raises(ValueError):
        report_id_for("m-")


def test_build_report():
    result = {
        "measurement_id": "m-a1b2",
        "metric": "latency",
        "sample_count": 2,
        "average_latency_ms": 15.0,
    }

    report = build_report("job-123", "m-a1b2", result)

    assert report == {
        "report_id": "r-a1b2",
        "job_id": "job-123",
        "measurement_id": "m-a1b2",
        "result": result,
    }

    # The original result dictionary is embedded, not copied.
    assert report["result"] is result


# ==============================================================================
# Successful processing
# ==============================================================================


def test_process_message_stores_report_and_completes_job(
    monkeypatch, redis_client, s3_client, valid_payload
):
    stored_reports = []

    def fake_put_json(bucket, key, data, client):
        stored_reports.append({
            "bucket": bucket,
            "key": key,
            "data": data,
            "client": client,
        })

    monkeypatch.setattr(report_main, "put_json", fake_put_json)

    process_message(valid_payload, redis_client, s3_client)

    # Verify that the report was stored in the correct bucket and key.
    assert len(stored_reports) == 1

    stored = stored_reports[0]

    assert stored["bucket"] == Buckets.REPORTS
    assert stored["key"] == report_key("r-a1b2")
    assert stored["client"] is s3_client

    assert stored["data"]["report_id"] == "r-a1b2"
    assert stored["data"]["job_id"] == "job-123"
    assert stored["data"]["measurement_id"] == "m-a1b2"

    # Verify that Redis was updated only after successful storage.
    assert redis_client.updates == [
        {
            "name": "job:job-123",
            "mapping": {
                "report_id": "r-a1b2",
                "status": JobStatus.COMPLETED.value,
            },
        }
    ]


# ==============================================================================
# Invalid messages
# ==============================================================================


def test_process_message_rejects_invalid_job_id(
    monkeypatch, redis_client, s3_client, valid_payload
):
    valid_payload["job_id"] = "invalid-123"

    def unexpected_put_json(**kwargs):
        pytest.fail("put_json should not be called for an invalid job_id")

    monkeypatch.setattr(report_main, "put_json", unexpected_put_json)

    with pytest.raises(ValueError, match="Invalid job_id"):
        process_message(valid_payload, redis_client, s3_client)

    assert redis_client.updates == []


# ==============================================================================
# Garage storage failure
# ==============================================================================


def test_process_message_handles_storage_failure(
    monkeypatch, redis_client, s3_client, valid_payload
):
    def failing_put_json(**kwargs):
        raise RuntimeError("Garage is unavailable")

    monkeypatch.setattr(report_main, "put_json", failing_put_json)

    with pytest.raises(ReportStorageError):
        process_message(valid_payload, redis_client, s3_client)

    # The job must be marked as failed if report storage fails.
    assert redis_client.updates == [
        {
            "name": "job:job-123",
            "mapping": {
                "status": JobStatus.FAILED_REPORT_STORAGE.value,
            },
        }
    ]


# ==============================================================================
# Duplicate message / idempotency
# ==============================================================================


def test_process_message_is_idempotent_for_duplicate_messages(
    monkeypatch, redis_client, s3_client, valid_payload
):
    stored_reports = []

    def fake_put_json(bucket, key, data, client=None):
        stored_reports.append({
            "bucket": bucket,
            "key": key,
            "data": data,
        })

    monkeypatch.setattr(report_main, "put_json", fake_put_json)

    # Simulate receiving the same RabbitMQ message twice.
    process_message(valid_payload, redis_client, s3_client)
    process_message(valid_payload, redis_client, s3_client)

    assert len(stored_reports) == 2

    # Both deliveries write to the same object key.
    assert stored_reports[0]["key"] == stored_reports[1]["key"]
    assert stored_reports[0]["key"] == report_key("r-a1b2")

    # Both deliveries produce the same report identity and content.
    assert stored_reports[0]["data"] == stored_reports[1]["data"]

    # Both deliveries update the same Redis job.
    assert len(redis_client.updates) == 2
    assert all(
        update["name"] == "job:job-123"
        for update in redis_client.updates
    )

    assert all(
        update["mapping"]["status"] == JobStatus.COMPLETED.value
        for update in redis_client.updates
    )


# ==============================================================================
# Consumer handler & callback tests
# ==============================================================================


def test_handle_message_ack_on_success(fake_redis, valid_payload):
    mock_s3 = MagicMock()
    channel = MagicMock()
    method = MagicMock(delivery_tag=1)
    props = MagicMock()
    body = json.dumps(valid_payload).encode("utf-8")

    handle_message(channel, method, props, body, redis_client=fake_redis, s3_client=mock_s3)

    channel.basic_ack.assert_called_once_with(delivery_tag=1)
    channel.basic_nack.assert_not_called()
    assert fake_redis.hgetall("job:job-123")["status"] == JobStatus.COMPLETED.value
    assert fake_redis.hgetall("job:job-123")["report_id"] == "r-a1b2"
    mock_s3.put_object.assert_called_once()


def test_handle_message_nack_no_requeue_on_malformed_json(fake_redis):
    mock_s3 = MagicMock()
    channel = MagicMock()
    method = MagicMock(delivery_tag=2)
    props = MagicMock()
    body = b"not-json"

    handle_message(channel, method, props, body, redis_client=fake_redis, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=2, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_nack_no_requeue_on_invalid_payload(fake_redis, valid_payload):
    valid_payload["job_id"] = "bad-prefix"
    mock_s3 = MagicMock()
    channel = MagicMock()
    method = MagicMock(delivery_tag=3)
    props = MagicMock()
    body = json.dumps(valid_payload).encode("utf-8")

    handle_message(channel, method, props, body, redis_client=fake_redis, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=3, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_ack_on_storage_error(fake_redis, valid_payload):
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = RuntimeError("S3 down")
    channel = MagicMock()
    method = MagicMock(delivery_tag=4)
    props = MagicMock()
    body = json.dumps(valid_payload).encode("utf-8")

    handle_message(channel, method, props, body, redis_client=fake_redis, s3_client=mock_s3)

    channel.basic_ack.assert_called_once_with(delivery_tag=4)
    channel.basic_nack.assert_not_called()
    assert fake_redis.hgetall("job:job-123")["status"] == JobStatus.FAILED_REPORT_STORAGE.value


def test_handle_message_nack_requeue_on_unexpected_error(monkeypatch, fake_redis, valid_payload):
    def raise_crash(*args, **kwargs):
        raise RuntimeError("Crash")

    monkeypatch.setattr("report_service.main.process_message", raise_crash)
    mock_s3 = MagicMock()
    channel = MagicMock()
    method = MagicMock(delivery_tag=5)
    props = MagicMock()
    body = json.dumps(valid_payload).encode("utf-8")

    handle_message(channel, method, props, body, redis_client=fake_redis, s3_client=mock_s3)

    channel.basic_nack.assert_called_once_with(delivery_tag=5, requeue=True)
    channel.basic_ack.assert_not_called()
