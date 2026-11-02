"""Lazy, confirmed RabbitMQ publishing for the API gateway."""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

import pika

from api_gateway.config import Settings


logger = logging.getLogger("api-gateway")


class PublishError(Exception):
    """Raised when a message cannot be published to RabbitMQ."""


class Publisher:
    """Thread-safe publisher with one reconnect attempt per publish call."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._connection: pika.BlockingConnection | None = None
        self._channel: Any | None = None
        self._declared_queues: set[str] = set()
        self._lock = threading.Lock()

    def _connect(self) -> None:
        credentials = pika.PlainCredentials(
            self.settings.rabbitmq_user,
            self.settings.rabbitmq_password,
        )
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.settings.rabbitmq_host,
                port=self.settings.rabbitmq_port,
                credentials=credentials,
                socket_timeout=3,
                connection_attempts=1,
                heartbeat=60,
            )
        )
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
                logger.debug("Could not close RabbitMQ connection", exc_info=True)

    def _publish_once(self, queue: str, payload: dict[str, Any]) -> None:
        if self._connection is None or self._channel is None:
            self._connect()
        if queue not in self._declared_queues:
            self._channel.queue_declare(queue=queue, durable=True)
            self._declared_queues.add(queue)

        published = self._channel.basic_publish(
            exchange="",
            routing_key=queue,
            mandatory=True,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
        if published is False:
            raise PublishError("RabbitMQ did not confirm the message")

    def publish(self, queue: str, payload: dict[str, Any]) -> None:
        """Publish one persistent JSON message, retrying one connection failure."""
        connection_errors = (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            OSError,
        )
        broker_errors = (pika.exceptions.AMQPError, OSError)

        with self._lock:
            for attempt in range(2):
                try:
                    self._publish_once(queue, payload)
                    return
                except connection_errors as exc:
                    self._reset_connection()
                    if attempt == 0:
                        continue
                    raise PublishError("RabbitMQ connection unavailable") from exc
                except broker_errors as exc:
                    raise PublishError("RabbitMQ publish failed") from exc
                except PublishError:
                    raise
                except Exception as exc:
                    raise PublishError("RabbitMQ publish failed") from exc

            raise PublishError("RabbitMQ connection unavailable")
