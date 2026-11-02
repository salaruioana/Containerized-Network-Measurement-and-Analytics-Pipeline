from __future__ import annotations

import json
from unittest.mock import MagicMock, Mock, patch

import pika
import pika.adapters.blocking_connection
import pika.exceptions
import pytest
from pydantic import BaseModel

from aic_common.contract import Queues
from aic_common.rabbitmq import (
    ALL_DURABLE_QUEUES,
    DEFAULT_RABBITMQ_HOST,
    DEFAULT_RABBITMQ_PASSWORD,
    DEFAULT_RABBITMQ_PORT,
    DEFAULT_RABBITMQ_USER,
    MalformedMessageError,
    PublishError,
    RabbitMQConsumer,
    RabbitMQError,
    RabbitMQPublisher,
    connect_rabbitmq,
    consume,
    declare_durable_queues,
    get_connection_parameters,
)


class SampleModel(BaseModel):
    job_id: str
    value: int


class FakeChannel:
    def __init__(self) -> None:
        self.declarations: list[dict] = []
        self.confirm_calls = 0
        self.published: list[dict] = []
        self.qos_calls: list[dict] = []
        self.consume_calls: list[dict] = []
        self.ack_calls: list[dict] = []
        self.nack_calls: list[dict] = []
        self.is_open = True
        self.publish_error: Exception | None = None
        self.publish_return: bool | None = None
        self.stop_consuming_called = False

    def confirm_delivery(self) -> None:
        self.confirm_calls += 1

    def queue_declare(self, **kwargs) -> None:
        self.declarations.append(kwargs)

    def basic_qos(self, **kwargs) -> None:
        self.qos_calls.append(kwargs)

    def basic_publish(self, **kwargs) -> bool:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append(kwargs)
        if self.publish_return is not None:
            return self.publish_return
        return True

    def basic_consume(self, **kwargs) -> str:
        self.consume_calls.append(kwargs)
        return "ctag-1"

    def basic_ack(self, delivery_tag: int, multiple: bool = False) -> None:
        self.ack_calls.append({"delivery_tag": delivery_tag, "multiple": multiple})

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        self.nack_calls.append({
            "delivery_tag": delivery_tag,
            "multiple": multiple,
            "requeue": requeue,
        })

    def stop_consuming(self) -> None:
        self.stop_consuming_called = True


class FakeConnection:
    def __init__(self, channel: FakeChannel | None = None) -> None:
        self.channel_instance = channel or FakeChannel()
        self.is_open = True
        self.closed = False

    def channel(self) -> FakeChannel:
        return self.channel_instance

    def close(self) -> None:
        self.closed = True
        self.is_open = False


# ==============================================================================
# Connection & Configuration Tests
# ==============================================================================


def test_get_connection_parameters_defaults(monkeypatch) -> None:
    monkeypatch.delenv("RABBITMQ_HOST", raising=False)
    monkeypatch.delenv("RABBITMQ_PORT", raising=False)
    monkeypatch.delenv("RABBITMQ_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_PASSWORD", raising=False)

    params = get_connection_parameters()
    assert params.host == DEFAULT_RABBITMQ_HOST
    assert params.port == DEFAULT_RABBITMQ_PORT
    assert params.credentials.username == DEFAULT_RABBITMQ_USER
    assert params.credentials.password == DEFAULT_RABBITMQ_PASSWORD
    assert params.heartbeat == 60


def test_get_connection_parameters_env_override(monkeypatch) -> None:
    monkeypatch.setenv("RABBITMQ_HOST", "mq.test.local")
    monkeypatch.setenv("RABBITMQ_PORT", "5673")
    monkeypatch.setenv("RABBITMQ_USER", "custom_user")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "secret")

    params = get_connection_parameters()
    assert params.host == "mq.test.local"
    assert params.port == 5673
    assert params.credentials.username == "custom_user"
    assert params.credentials.password == "secret"


def test_get_connection_parameters_explicit_arguments() -> None:
    params = get_connection_parameters(
        host="host1",
        port=5674,
        user="u1",
        password="p1",
        heartbeat=120,
    )
    assert params.host == "host1"
    assert params.port == 5674
    assert params.credentials.username == "u1"
    assert params.credentials.password == "p1"
    assert params.heartbeat == 120


def test_connect_rabbitmq_success(monkeypatch) -> None:
    fake_conn = FakeConnection()
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: fake_conn)

    conn = connect_rabbitmq(retry_interval=0.01, max_retries=1)
    assert conn is fake_conn


def test_connect_rabbitmq_retries_until_success(monkeypatch) -> None:
    fake_conn = FakeConnection()
    attempts = [0]

    def mock_connect(params):
        attempts[0] += 1
        if attempts[0] < 3:
            raise pika.exceptions.AMQPConnectionError("Connection refused")
        return fake_conn

    monkeypatch.setattr(pika, "BlockingConnection", mock_connect)

    conn = connect_rabbitmq(retry_interval=0.001, max_retries=5)
    assert conn is fake_conn
    assert attempts[0] == 3


def test_connect_rabbitmq_exceeds_max_retries(monkeypatch) -> None:
    def mock_connect(params):
        raise pika.exceptions.AMQPConnectionError("Connection refused")

    monkeypatch.setattr(pika, "BlockingConnection", mock_connect)

    with pytest.raises(RabbitMQError, match="Failed to connect to RabbitMQ after 2 attempts"):
        connect_rabbitmq(retry_interval=0.001, max_retries=2)


def test_connect_rabbitmq_stop_check(monkeypatch) -> None:
    stopped = True
    with pytest.raises(RabbitMQError, match="Connection cancelled"):
        connect_rabbitmq(retry_interval=0.001, stop_check=lambda: stopped)


# ==============================================================================
# Queue Declarations Tests
# ==============================================================================


def test_declare_durable_queues() -> None:
    channel = FakeChannel()
    declare_durable_queues(channel)

    declared_queues = [item["queue"] for item in channel.declarations]
    assert set(declared_queues) == set(ALL_DURABLE_QUEUES)
    for decl in channel.declarations:
        assert decl["durable"] is True


# ==============================================================================
# Publisher Tests
# ==============================================================================


def test_publisher_confirms_declares_and_persists(monkeypatch) -> None:
    channel = FakeChannel()
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()
    pub.publish("test.queue", {"foo": "bar"})
    pub.publish("test.queue", {"foo": "baz"})

    assert channel.confirm_calls == 1
    assert channel.declarations == [{"queue": "test.queue", "durable": True}]
    assert len(channel.published) == 2

    first = channel.published[0]
    assert first["routing_key"] == "test.queue"
    assert first["mandatory"] is True
    assert json.loads(first["body"].decode()) == {"foo": "bar"}
    assert first["properties"].content_type == "application/json"
    assert first["properties"].delivery_mode == 2


def test_publisher_payload_serialization(monkeypatch) -> None:
    channel = FakeChannel()
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()

    # Pydantic model
    model = SampleModel(job_id="job-1", value=42)
    pub.publish("test.queue", model)
    assert json.loads(channel.published[-1]["body"].decode()) == {"job_id": "job-1", "value": 42}

    # String
    pub.publish("test.queue", "raw-string")
    assert channel.published[-1]["body"] == b"raw-string"

    # Bytes
    pub.publish("test.queue", b"raw-bytes")
    assert channel.published[-1]["body"] == b"raw-bytes"


def test_publisher_raises_on_nack_error(monkeypatch) -> None:
    channel = FakeChannel()
    channel.publish_error = pika.exceptions.NackError([])
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()
    with pytest.raises(PublishError, match="Broker did not confirm delivery"):
        pub.publish("test.queue", {"msg": "hi"})


def test_publisher_raises_on_unroutable_error(monkeypatch) -> None:
    channel = FakeChannel()
    channel.publish_error = pika.exceptions.UnroutableError([])
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()
    with pytest.raises(PublishError, match="Broker did not confirm delivery"):
        pub.publish("test.queue", {"msg": "hi"})


def test_publisher_raises_when_publish_returns_false(monkeypatch) -> None:
    channel = FakeChannel()
    channel.publish_return = False
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()
    with pytest.raises(PublishError, match="Broker rejected/nacked message"):
        pub.publish("test.queue", {"msg": "hi"})


def test_publisher_reconnects_once_on_connection_error(monkeypatch) -> None:
    first_ch = FakeChannel()
    first_ch.publish_error = pika.exceptions.AMQPConnectionError("Connection lost")
    second_ch = FakeChannel()

    connections = [FakeConnection(first_ch), FakeConnection(second_ch)]
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: connections.pop(0))

    pub = RabbitMQPublisher()
    pub.publish("test.queue", {"data": 123})

    assert len(first_ch.published) == 0
    assert len(second_ch.published) == 1
    assert json.loads(second_ch.published[0]["body"].decode()) == {"data": 123}


def test_publisher_fails_after_second_connection_error(monkeypatch) -> None:
    first_ch = FakeChannel()
    first_ch.publish_error = pika.exceptions.AMQPConnectionError("Err 1")
    second_ch = FakeChannel()
    second_ch.publish_error = pika.exceptions.AMQPConnectionError("Err 2")

    connections = [FakeConnection(first_ch), FakeConnection(second_ch)]
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: connections.pop(0))

    pub = RabbitMQPublisher()
    with pytest.raises(PublishError, match="RabbitMQ connection unavailable"):
        pub.publish("test.queue", {"data": 123})


def test_publisher_context_manager(monkeypatch) -> None:
    channel = FakeChannel()
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    with RabbitMQPublisher() as pub:
        pub.publish("test.queue", {"foo": "bar"})

    assert conn.closed is True


# ==============================================================================
# Consumer Tests
# ==============================================================================


def test_consumer_manual_ack_on_success() -> None:
    received = []

    def handler(payload: dict) -> None:
        received.append(payload)

    consumer = RabbitMQConsumer(queue="test.queue", handler=handler)
    channel = FakeChannel()
    method = Mock(delivery_tag=101)
    props = Mock()
    body = json.dumps({"job_id": "job-100", "status": "ok"}).encode()

    consumer._dispatch_message(channel, method, props, body)

    assert received == [{"job_id": "job-100", "status": "ok"}]
    assert channel.ack_calls == [{"delivery_tag": 101, "multiple": False}]
    assert channel.nack_calls == []


def test_consumer_nack_no_requeue_on_malformed_json() -> None:
    handler = Mock()
    consumer = RabbitMQConsumer(queue="test.queue", handler=handler)
    channel = FakeChannel()
    method = Mock(delivery_tag=102)
    props = Mock()
    body = b"{not-valid-json"

    consumer._dispatch_message(channel, method, props, body)

    handler.assert_not_called()
    assert channel.ack_calls == []
    assert channel.nack_calls == [{"delivery_tag": 102, "multiple": False, "requeue": False}]


def test_consumer_nack_no_requeue_on_malformed_message_error() -> None:
    def handler(payload: dict) -> None:
        raise MalformedMessageError("Invalid schema")

    consumer = RabbitMQConsumer(queue="test.queue", handler=handler)
    channel = FakeChannel()
    method = Mock(delivery_tag=103)
    props = Mock()
    body = json.dumps({"missing": "field"}).encode()

    consumer._dispatch_message(channel, method, props, body)

    assert channel.ack_calls == []
    assert channel.nack_calls == [{"delivery_tag": 103, "multiple": False, "requeue": False}]


def test_consumer_nack_requeue_on_unexpected_error() -> None:
    def handler(payload: dict) -> None:
        raise RuntimeError("Transient database error")

    consumer = RabbitMQConsumer(queue="test.queue", handler=handler, requeue_on_error=True)
    channel = FakeChannel()
    method = Mock(delivery_tag=104)
    props = Mock()
    body = json.dumps({"job_id": "job-100"}).encode()

    consumer._dispatch_message(channel, method, props, body)

    assert channel.ack_calls == []
    assert channel.nack_calls == [{"delivery_tag": 104, "multiple": False, "requeue": True}]


def test_consumer_supports_four_parameter_handler() -> None:
    captured = {}

    def handler(ch, method, properties, raw_body) -> None:
        captured["ch"] = ch
        captured["tag"] = method.delivery_tag
        captured["body"] = raw_body

    consumer = RabbitMQConsumer(queue="test.queue", handler=handler)
    channel = FakeChannel()
    method = Mock(delivery_tag=105)
    props = Mock()
    body = b'{"msg": "raw"}'

    consumer._dispatch_message(channel, method, props, body)

    assert captured["ch"] is channel
    assert captured["tag"] == 105
    assert captured["body"] == body
    assert channel.ack_calls == [{"delivery_tag": 105, "multiple": False}]


def test_consumer_setup_qos_and_durable_queues(monkeypatch) -> None:
    channel = FakeChannel()
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    handler = Mock()
    consumer = RabbitMQConsumer(queue="test.queue", handler=handler, prefetch_count=1)

    # Trigger stop immediately after starting consumption
    def fake_start_consuming():
        consumer.request_stop()

    channel.start_consuming = fake_start_consuming

    consumer.start()

    assert channel.qos_calls == [{"prefetch_count": 1}]
    declared_queues = [item["queue"] for item in channel.declarations]
    assert set(declared_queues) == set(ALL_DURABLE_QUEUES)
    assert len(channel.consume_calls) == 1
    assert channel.consume_calls[0]["queue"] == "test.queue"
    assert channel.consume_calls[0]["auto_ack"] is False
    assert consumer.stop_requested is True


def test_consumer_request_stop_stops_consuming() -> None:
    consumer = RabbitMQConsumer(queue="test.queue", handler=Mock())
    channel = FakeChannel()
    consumer.channel = channel

    consumer.request_stop()
    assert consumer.stop_requested is True
    assert channel.stop_consuming_called is True


def test_publisher_reconnects_on_closed_channel_and_channel_error(monkeypatch) -> None:
    first_ch = FakeChannel()
    first_ch.publish_error = pika.exceptions.ChannelWrongStateError("Channel closed")
    second_ch = FakeChannel()
    third_ch = FakeChannel()

    connections = [FakeConnection(first_ch), FakeConnection(second_ch), FakeConnection(third_ch)]
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: connections.pop(0))

    params = get_connection_parameters()
    pub = RabbitMQPublisher(parameters=params)
    # 1. Retries once when ChannelWrongStateError occurs
    pub.publish("test.queue", {"data": 1})
    assert len(second_ch.published) == 1

    # 2. Reconnects proactively if channel.is_closed is True
    second_ch.is_closed = True  # type: ignore[attr-defined]
    pub.publish("test.queue", [1, 2, 3])
    assert len(third_ch.published) == 1
    assert json.loads(third_ch.published[0]["body"].decode()) == [1, 2, 3]


def test_publisher_legacy_dict_primitive_and_non_retryable_amqp_error(monkeypatch) -> None:
    channel = FakeChannel()
    conn = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conn)

    pub = RabbitMQPublisher()

    class LegacyModel:
        def dict(self, exclude_none: bool = False):
            return {"legacy": True}

    pub.publish("test.queue", LegacyModel())
    assert json.loads(channel.published[-1]["body"].decode()) == {"legacy": True}

    pub.publish("test.queue", 42)
    assert json.loads(channel.published[-1]["body"].decode()) == 42

    # Generic non-connection AMQPError converts to PublishError without second reconnect
    channel.publish_error = pika.exceptions.AMQPError("Generic protocol error")
    with pytest.raises(PublishError, match="Publish failed"):
        pub.publish("test.queue", {"a": 1})


def test_consumer_dispatch_two_three_param_and_raw_bytes() -> None:
    captured = {}

    def two_param_handler(data, properties) -> None:
        captured["two"] = (data, properties)

    def three_param_handler(data, properties, method) -> None:
        captured["three"] = (data, properties, method.delivery_tag)

    channel = FakeChannel()
    method = Mock(delivery_tag=201)
    props = Mock()

    c2 = RabbitMQConsumer(queue="test.queue", handler=two_param_handler, auto_decode_json=False)
    c2._dispatch_message(channel, method, props, b"raw-payload")
    assert captured["two"] == (b"raw-payload", props)

    c3 = RabbitMQConsumer(queue="test.queue", handler=three_param_handler)
    c3._dispatch_message(channel, method, props, b'{"x": 1}')
    assert captured["three"] == ({"x": 1}, props, 201)


def test_consumer_stops_cleanly_when_cancelled_during_connect_retry(monkeypatch) -> None:
    registered_signals: dict[int, object] = {}
    monkeypatch.setattr(
        "aic_common.rabbitmq.signal.signal",
        lambda sig, handler: registered_signals.setdefault(sig, handler),
    )
    consumer = RabbitMQConsumer(
        queue="test.queue",
        handler=Mock(),
        retry_interval=0.001,
        declare_all_queues=False,
    )

    def fail_then_stop(params):
        # Invoke the registered signal handler as if SIGTERM arrived
        for sig, sig_handler in registered_signals.items():
            sig_handler(sig, None)  # type: ignore[operator]
        raise pika.exceptions.AMQPConnectionError("Broker still starting")

    monkeypatch.setattr(pika, "BlockingConnection", fail_then_stop)

    # Must return cleanly without raising RabbitMQError
    consumer.start(setup_signals=True)
    assert consumer.stop_requested is True
    assert len(registered_signals) >= 1


def test_consumer_reconnects_after_connection_drop_and_consume_helper(monkeypatch) -> None:
    ch1 = FakeChannel()
    ch2 = FakeChannel()
    conns = [FakeConnection(ch1), FakeConnection(ch2)]
    monkeypatch.setattr(pika, "BlockingConnection", lambda params: conns.pop(0))

    consumer_ref: list[RabbitMQConsumer] = []

    def drop_once():
        raise pika.exceptions.AMQPConnectionError("Dropped mid-consume")

    def stop_second():
        # Also invoke registered callback once before stopping
        cb = ch2.consume_calls[0]["on_message_callback"]
        cb(ch2, Mock(delivery_tag=1), Mock(), b'{"ok": true}')
        consumer_ref[0].request_stop()

    ch1.start_consuming = drop_once
    ch2.start_consuming = stop_second

    received = []
    c = consume("single.queue", lambda d: received.append(d), start_now=False)
    c.retry_interval = 0.001
    c.declare_all_queues = False
    consumer_ref.append(c)
    c.start()

    assert received == [{"ok": True}]
    assert ch2.declarations == [{"queue": "single.queue", "durable": True}]
    assert ch2.ack_calls == [{"delivery_tag": 1, "multiple": False}]


