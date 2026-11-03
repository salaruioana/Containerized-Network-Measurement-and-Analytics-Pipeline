"""Tests for the analytics consumer (#30, #31, #32, #33)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import boto3
import fakeredis
import pytest
import redis
from moto import mock_aws

from aic_common.contract import Buckets, JobStatus, Queues
from aic_common.redis_state import create_job, get_job, set_job_status
from aic_common.s3 import put_json
from analytics_service.main import (
    AnalysisError,
    MalformedRequestError,
    handle_message,
    process_message,
)

RAW = {
    "measurement_id": "m-a1b2",
    "source_host": "h1",
    "target_host": "h2",
    "metric": "latency",
    "values_ms": [10, 11, 9, 45, 12],
    "unit": "ms",
    "packets_sent": 5,
    "packets_received": 5,
}
REQUEST = {"job_id": "job-77", "measurement_id": "m-a1b2", "analysis": "latency_summary", "threshold_ms": 30}


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=Buckets.MEASUREMENTS)
        yield client


@pytest.fixture
def stored(redis_client, s3_client):
    """A measurement as the Storage Service leaves it, and a job in `stored`."""
    put_json(Buckets.MEASUREMENTS, "h1-h2/m-a1b2.json", RAW, client=s3_client)
    redis_client.hset("meas:m-a1b2", mapping={"object_path": "measurements/h1-h2/m-a1b2.json"})
    create_job(redis_client, "job-77", "m-a1b2")
    set_job_status(redis_client, "job-77", JobStatus.STORED)


def published(channel):
    """The message bodies published on the mocked channel."""
    return [json.loads(c.kwargs["body"]) for c in channel.basic_publish.call_args_list]


# --- process_message: the logic ---------------------------------------------


def test_happy_path_publishes_flat_result_and_sets_analyzed(redis_client, s3_client, stored):
    channel = MagicMock()
    process_message(REQUEST, redis_client, s3_client, channel)

    assert get_job(redis_client, "job-77")["status"] == "analyzed"
    [message] = published(channel)
    assert message["job_id"] == "job-77"
    assert message["average_latency_ms"] == 17.4
    assert message["quality"] == "critical"
    assert "throughput_avg_mbps" not in message  # no null fields
    assert channel.basic_publish.call_args.kwargs["routing_key"] == Queues.ANALYSIS_COMPLETED


def test_workflow_c_goes_from_queued_to_analyzed(redis_client, s3_client, stored):
    redis_client.hset("job:job-88", mapping={"measurement_id": "m-a1b2", "status": "queued"})
    process_message({**REQUEST, "job_id": "job-88"}, redis_client, s3_client, MagicMock())
    assert get_job(redis_client, "job-88")["status"] == "analyzed"


def test_missing_metadata_sets_failed_analysis(redis_client, s3_client):
    create_job(redis_client, "job-77", "m-a1b2")
    with pytest.raises(AnalysisError):
        process_message(REQUEST, redis_client, s3_client, MagicMock())
    assert get_job(redis_client, "job-77")["status"] == "failed_analysis"


def test_redelivery_after_completed_is_skipped(redis_client, s3_client, stored):
    set_job_status(redis_client, "job-77", JobStatus.ANALYZED)
    set_job_status(redis_client, "job-77", JobStatus.COMPLETED)
    channel = MagicMock()
    assert process_message(REQUEST, redis_client, s3_client, channel) is None
    assert get_job(redis_client, "job-77")["status"] == "completed"
    channel.basic_publish.assert_not_called()


def test_redelivery_while_analyzed_publishes_the_same_result_again(redis_client, s3_client, stored):
    first, second = MagicMock(), MagicMock()
    process_message(REQUEST, redis_client, s3_client, first)
    process_message(REQUEST, redis_client, s3_client, second)  # e.g. crash before the ack
    assert published(first) == published(second)


def test_malformed_request_raises(redis_client, s3_client):
    with pytest.raises(MalformedRequestError):
        process_message({"measurement_id": "m-a1b2"}, redis_client, s3_client, MagicMock())


# --- handle_message: ack or nack --------------------------------------------


def test_handle_message_acks_on_success(redis_client, s3_client, stored):
    ch, method = MagicMock(), MagicMock()
    handle_message(ch, method, None, json.dumps(REQUEST).encode(), redis_client, s3_client)
    ch.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)


def test_handle_message_drops_broken_json(redis_client, s3_client):
    ch, method = MagicMock(), MagicMock()
    handle_message(ch, method, None, b"not json", redis_client, s3_client)
    ch.basic_nack.assert_called_once_with(delivery_tag=method.delivery_tag, requeue=False)


def test_handle_message_acks_failed_analysis(redis_client, s3_client):
    create_job(redis_client, "job-77", "m-a1b2")
    ch, method = MagicMock(), MagicMock()
    handle_message(ch, method, None, json.dumps(REQUEST).encode(), redis_client, s3_client)
    ch.basic_ack.assert_called_once()
    assert get_job(redis_client, "job-77")["status"] == "failed_analysis"


def test_handle_message_requeues_on_redis_outage(s3_client):
    broken = MagicMock()
    broken.hgetall.side_effect = redis.ConnectionError("down")
    ch, method = MagicMock(), MagicMock()
    handle_message(ch, method, None, json.dumps(REQUEST).encode(), broken, s3_client)
    ch.basic_nack.assert_called_once_with(delivery_tag=method.delivery_tag, requeue=True)