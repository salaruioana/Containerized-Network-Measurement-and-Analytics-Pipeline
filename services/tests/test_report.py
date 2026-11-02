import pytest

from report_service import main as report_main
from report_service.main import (
    ReportStorageError,
    build_report,
    process_message,
    report_id_for,
)
from aic_common.contract import Buckets, JobStatus
from aic_common.s3 import report_key


class FakeRedis:
    """Simple Redis mock that records hash updates."""

    def __init__(self):
        self.updates = []

    def hset(self, name, mapping):
        self.updates.append({
            "name": name,
            "mapping": mapping,
        })


@pytest.fixture
def redis_client():
    return FakeRedis()


@pytest.fixture
def s3_client():
    # process_message passes this object to put_json.
    return object()


@pytest.fixture
def valid_payload():
    return {
        "job_id": "job-123",
        "measurement_id": "m-a1b2",
        "metric": "latency",
        "sample_count": 2,
        "average_latency_ms": 15.0,
    }


# Report ID and report construction

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


# Successful processing


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


# Invalid messages


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


# Garage storage failure

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


# Duplicate message / idempotency

def test_process_message_is_idempotent_for_duplicate_messages(
    monkeypatch, redis_client, s3_client, valid_payload
):
    stored_reports = []

    def fake_put_json(bucket, key, data, client):
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