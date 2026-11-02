"""Lazy, confirmed RabbitMQ publishing for the API gateway.

Uses the shared RabbitMQ helper from aic_common.rabbitmq.
"""

from __future__ import annotations

from aic_common.rabbitmq import PublishError, RabbitMQPublisher

# Compatible alias for API Gateway
Publisher = RabbitMQPublisher

__all__ = ["PublishError", "Publisher"]
