"""Unit tests for Measurement Storage Service (task #18).

Tests message parsing, validation, manual ACK/NACK handling, and graceful shutdown.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from aic_common.contract import Queues
from storage_service.main import (
    ConsumerContext,
    StorageMessage,
    handle_message,
    parse_storage_message,
    process_message,
)


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


def test_handle_message_ack_on_success(valid_flat_payload):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 42
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    handle_message(channel, method, properties, body)

    channel.basic_ack.assert_called_once_with(delivery_tag=42)
    channel.basic_nack.assert_not_called()


def test_handle_message_nack_no_requeue_on_malformed_json():
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 99
    properties = MagicMock()
    body = b"not a json object {"

    handle_message(channel, method, properties, body)

    channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_nack_no_requeue_on_schema_violation(valid_flat_payload):
    valid_flat_payload["job_id"] = "bad-prefix"
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 100
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    handle_message(channel, method, properties, body)

    channel.basic_nack.assert_called_once_with(delivery_tag=100, requeue=False)
    channel.basic_ack.assert_not_called()


def test_handle_message_nack_requeue_on_unexpected_error(monkeypatch, valid_flat_payload):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 101
    properties = MagicMock()
    body = json.dumps(valid_flat_payload).encode("utf-8")

    def raise_crash(msg, **kwargs):
        raise RuntimeError("Unexpected simulated system crash")

    monkeypatch.setattr("storage_service.main.process_message", raise_crash)

    handle_message(channel, method, properties, body)

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

