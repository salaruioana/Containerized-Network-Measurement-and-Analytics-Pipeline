"""Shared RabbitMQ client helper for publishing and consuming messages.

See Task group-05#5.
Provides resilient connection management with retry, durable queue declaration,
confirmed persistent publishing, and fair-dispatch manual-ACK consumption.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import signal
import threading
import time
from typing import Any, Callable, Iterable

import pika
import pika.adapters.blocking_connection
import pika.exceptions

from aic_common.contract import Queues

logger = logging.getLogger("aic-rabbitmq")

ALL_DURABLE_QUEUES: tuple[str, ...] = (
    Queues.MEASUREMENT_CREATED,
    Queues.ANALYSIS_REQUESTED,
    Queues.ANALYSIS_COMPLETED,
    Queues.MEASUREMENT_FAILED,
)

DEFAULT_RABBITMQ_HOST = "rabbitmq"
DEFAULT_RABBITMQ_PORT = 5672
DEFAULT_RABBITMQ_USER = "guest"
DEFAULT_RABBITMQ_PASSWORD = "guest"


class RabbitMQError(Exception):
    """Base exception for RabbitMQ helper operations."""


class PublishError(RabbitMQError):
    """Raised when a message cannot be published or confirmed by the broker."""


class MalformedMessageError(RabbitMQError):
    """Raised when a consumed message is structurally invalid and must not be requeued."""


def get_connection_parameters(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    heartbeat: int = 60,
    blocked_connection_timeout: int = 30,
    socket_timeout: int | float = 3,
    connection_attempts: int = 1,
) -> pika.ConnectionParameters:
    """Build pika ConnectionParameters from arguments or environment variables."""
    resolved_host = host or os.getenv("RABBITMQ_HOST", DEFAULT_RABBITMQ_HOST)
    resolved_port = port if port is not None else int(os.getenv("RABBITMQ_PORT", str(DEFAULT_RABBITMQ_PORT)))
    resolved_user = user or os.getenv("RABBITMQ_USER", DEFAULT_RABBITMQ_USER)
    resolved_password = password or os.getenv("RABBITMQ_PASSWORD", DEFAULT_RABBITMQ_PASSWORD)

    credentials = pika.PlainCredentials(resolved_user, resolved_password)
    return pika.ConnectionParameters(
        host=resolved_host,
        port=resolved_port,
        credentials=credentials,
        heartbeat=heartbeat,
        blocked_connection_timeout=blocked_connection_timeout,
        socket_timeout=socket_timeout,
        connection_attempts=connection_attempts,
    )


def connect_rabbitmq(
    parameters: pika.ConnectionParameters | None = None,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    retry_interval: float = 5.0,
    max_retries: int | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> pika.BlockingConnection:
    """Connect to RabbitMQ, retrying while broker is unavailable.

    If max_retries is specified, stops and raises the last connection error.
    """
    params = parameters or get_connection_parameters(host=host, port=port, user=user, password=password)
    attempts = 0

    while True:
        attempts += 1
        if stop_check and stop_check():
            raise RabbitMQError("Connection cancelled by stop check")

        try:
            connection = pika.BlockingConnection(params)
            logger.info("Connected to RabbitMQ at %s:%s", params.host, params.port)
            return connection
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError, OSError) as exc:
            if max_retries is not None and attempts >= max_retries:
                logger.error(
                    "RabbitMQ connection to %s:%s failed after %d attempts: %s",
                    params.host,
                    params.port,
                    attempts,
                    exc,
                )
                raise RabbitMQError(f"Failed to connect to RabbitMQ after {attempts} attempts") from exc

            logger.warning(
                "RabbitMQ at %s:%s unavailable (%s). Retrying in %.1f seconds... (attempt %d)",
                params.host,
                params.port,
                exc,
                retry_interval,
                attempts,
            )
            time.sleep(retry_interval)


def declare_durable_queues(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queues: Iterable[str] = ALL_DURABLE_QUEUES,
) -> None:
    """Declare durable queues idempotently on the channel."""
    for queue in queues:
        channel.queue_declare(queue=queue, durable=True)


class RabbitMQPublisher:
    """Thread-safe publisher supporting publisher confirms and automatic reconnect."""

    def __init__(
        self,
        settings: Any = None,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        parameters: pika.ConnectionParameters | None = None,
    ) -> None:
        self.settings = settings
        if settings is not None:
            self.parameters = get_connection_parameters(
                host=getattr(settings, "rabbitmq_host", host),
                port=getattr(settings, "rabbitmq_port", port),
                user=getattr(settings, "rabbitmq_user", user),
                password=getattr(settings, "rabbitmq_password", password),
            )
        elif parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = get_connection_parameters(host=host, port=port, user=user, password=password)

        self._connection: pika.BlockingConnection | None = None
        self._channel: Any | None = None
        self._declared_queues: set[str] = set()
        self._lock = threading.Lock()

    def _connect(self) -> None:
        self._connection = pika.BlockingConnection(self.parameters)
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()
        self._declared_queues.clear()

    def _reset_connection(self) -> None:
        connection = self._connection
        self._connection = None
        self._channel = None
        self._declared_queues.clear()
        if connection is not None:
            try:
                connection.close()
            except Exception:
                logger.debug("Could not close broken RabbitMQ connection", exc_info=True)

    def _publish_once(self, queue: str, payload: Any, mandatory: bool = True) -> None:
        if (
            self._connection is None
            or getattr(self._connection, "is_closed", False)
            or self._channel is None
            or getattr(self._channel, "is_closed", False)
        ):
            self._connect()

        if queue not in self._declared_queues:
            self._channel.queue_declare(queue=queue, durable=True)
            self._declared_queues.add(queue)

        if hasattr(payload, "model_dump"):
            try:
                dumped = payload.model_dump(mode="json", exclude_none=True)
            except (TypeError, ValueError):
                dumped = payload.model_dump(exclude_none=True)
            body = json.dumps(dumped, ensure_ascii=False, allow_nan=False).encode("utf-8")
        elif hasattr(payload, "dict"):
            body = json.dumps(
                payload.dict(exclude_none=True),
                ensure_ascii=False,
                allow_nan=False,
                default=str,
            ).encode("utf-8")
        elif isinstance(payload, (dict, list)):
            body = json.dumps(payload, ensure_ascii=False, allow_nan=False, default=str).encode("utf-8")
        elif isinstance(payload, str):
            body = payload.encode("utf-8")
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, ensure_ascii=False, allow_nan=False, default=str).encode("utf-8")

        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # persistent
        )

        try:
            confirmed = self._channel.basic_publish(
                exchange="",
                routing_key=queue,
                mandatory=mandatory,
                body=body,
                properties=properties,
            )
            if confirmed is False:
                raise PublishError(f"Broker rejected/nacked message for queue '{queue}'")
        except pika.exceptions.UnroutableError as exc:
            self._declared_queues.discard(queue)
            raise PublishError(f"Broker did not confirm delivery to queue '{queue}': {exc}") from exc
        except pika.exceptions.NackError as exc:
            raise PublishError(f"Broker did not confirm delivery to queue '{queue}': {exc}") from exc

    def publish(self, queue: str, payload: Any, mandatory: bool = True) -> None:
        """Publish one persistent message with publisher confirms and 1 reconnect retry."""
        connection_errors = (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.ChannelClosed,
            OSError,
        )

        with self._lock:
            for attempt in range(2):
                try:
                    self._publish_once(queue, payload, mandatory=mandatory)
                    return
                except connection_errors as exc:
                    self._reset_connection()
                    if attempt == 0:
                        continue
                    raise PublishError(f"RabbitMQ connection unavailable for queue '{queue}'") from exc
                except (pika.exceptions.AMQPError, OSError) as exc:
                    self._reset_connection()
                    raise PublishError(f"Publish failed for queue '{queue}': {exc}") from exc

    def close(self) -> None:
        """Close connection and channel."""
        with self._lock:
            self._reset_connection()

    def __enter__(self) -> RabbitMQPublisher:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# Alias for compatibility with api_gateway.publisher.Publisher
Publisher = RabbitMQPublisher


class RabbitMQConsumer:
    """Consumes from a queue with manual ack and prefetch_count=1."""

    def __init__(
        self,
        queue: str,
        handler: Callable[..., Any],
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        parameters: pika.ConnectionParameters | None = None,
        prefetch_count: int = 1,
        auto_decode_json: bool = True,
        requeue_on_error: bool = True,
        auto_ack_on_success: bool = True,
        retry_interval: float = 5.0,
        declare_all_queues: bool = True,
    ) -> None:
        self.queue = queue
        self.handler = handler
        self.parameters = parameters or get_connection_parameters(host=host, port=port, user=user, password=password)
        self.prefetch_count = prefetch_count
        self.auto_decode_json = auto_decode_json
        self.requeue_on_error = requeue_on_error
        self.auto_ack_on_success = auto_ack_on_success
        self.retry_interval = retry_interval
        self.declare_all_queues = declare_all_queues

        self.stop_requested = False
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def request_stop(self) -> None:
        """Request the consumer to stop gracefully."""
        self.stop_requested = True
        if self.connection and getattr(self.connection, "is_open", False):
            try:
                if self.channel and getattr(self.channel, "is_open", False):
                    self.connection.add_callback_threadsafe(self.channel.stop_consuming)
            except Exception:
                try:
                    if self.channel and self.channel.is_open:
                        self.channel.stop_consuming()
                except Exception:
                    pass
        elif self.channel and getattr(self.channel, "is_open", False):
            try:
                self.channel.stop_consuming()
            except Exception:
                pass

    stop = request_stop

    def _dispatch_message(
        self,
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        if self.auto_decode_json:
            try:
                data = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
            except Exception as exc:
                logger.error(
                    "Rejected malformed JSON on queue %s (requeue=False): %s",
                    self.queue,
                    exc,
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
        else:
            data = body

        try:
            sig = inspect.signature(self.handler)
            num_params = len(sig.parameters)
            if num_params == 4:
                self.handler(ch, method, properties, body)
            elif num_params == 3:
                self.handler(data, properties, method)
            elif num_params == 2:
                self.handler(data, properties)
            else:
                self.handler(data)

            if self.auto_ack_on_success:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except MalformedMessageError as exc:
            logger.error("Message rejected on queue %s (requeue=False): %s", self.queue, exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as exc:
            logger.exception("Error processing message on queue %s: %s", self.queue, exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=self.requeue_on_error)

    def start(self, setup_signals: bool = False) -> None:
        """Start consuming loop, reconnecting until stopped."""
        if setup_signals:
            def _sig_handler(signum: int, frame: Any) -> None:
                logger.info("Signal %s received. Stopping consumer on %s...", signum, self.queue)
                self.request_stop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    signal.signal(sig, _sig_handler)
                except (ValueError, AttributeError):
                    pass

        while not self.stop_requested:
            try:
                self.connection = connect_rabbitmq(
                    parameters=self.parameters,
                    retry_interval=self.retry_interval,
                    stop_check=lambda: self.stop_requested,
                )
                self.channel = self.connection.channel()

                if self.declare_all_queues:
                    declare_durable_queues(self.channel)
                else:
                    self.channel.queue_declare(queue=self.queue, durable=True)

                self.channel.basic_qos(prefetch_count=self.prefetch_count)

                def _callback(ch: Any, method: Any, properties: Any, body: Any) -> None:
                    self._dispatch_message(ch, method, properties, body)

                self.channel.basic_consume(
                    queue=self.queue,
                    on_message_callback=_callback,
                    auto_ack=False,
                )

                logger.info("Consuming queue %s (prefetch=%d)...", self.queue, self.prefetch_count)
                self.channel.start_consuming()

            except RabbitMQError:
                if self.stop_requested:
                    break
                raise
            except (pika.exceptions.AMQPError, OSError) as exc:
                if self.stop_requested:
                    break
                logger.warning(
                    "RabbitMQ consumer connection dropped (%s). Reconnecting in %.1fs...",
                    exc,
                    self.retry_interval,
                )
                time.sleep(self.retry_interval)
            finally:
                if self.connection and self.connection.is_open:
                    try:
                        self.connection.close()
                    except Exception:
                        pass

        logger.info("Consumer on queue %s stopped cleanly.", self.queue)


def consume(
    queue: str,
    handler: Callable[..., Any],
    *,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    prefetch_count: int = 1,
    auto_decode_json: bool = True,
    requeue_on_error: bool = True,
    auto_ack_on_success: bool = True,
    start_now: bool = True,
    setup_signals: bool = False,
) -> RabbitMQConsumer:
    """Create and optionally start a RabbitMQConsumer for the given queue."""
    consumer = RabbitMQConsumer(
        queue=queue,
        handler=handler,
        host=host,
        port=port,
        user=user,
        password=password,
        prefetch_count=prefetch_count,
        auto_decode_json=auto_decode_json,
        requeue_on_error=requeue_on_error,
        auto_ack_on_success=auto_ack_on_success,
    )
    if start_now:
        consumer.start(setup_signals=setup_signals)
    return consumer

