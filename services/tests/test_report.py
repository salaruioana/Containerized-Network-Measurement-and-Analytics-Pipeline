
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from aic_common.contract import Buckets, JobStatus
from aic_common.s3 import exists, get_json
from report_service.main import (
    ReportStorageError,
    build_report,
    handle_message,
    process_message,
    report_id_for,
)


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(self, name: str, mapping: dict | None = None, **kwargs) -> int:
        if name not in self.hashes:
            self.hashes[name] = {}
        if mapping:
            self.hashes[name].update({str(k): str(v) for k, v in mapping.items()})
        return len(self.hashes[name])

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def valid_payload() -> dict:
    return {
        "job_id": "job-123",
        "measurement_id": "m-a1b2",
        "metric": "latency",
        "sample_count": 2,
        "average_latency_ms": 15.0,
    }


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
    assert report["result"] is result


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
