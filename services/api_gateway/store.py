"""Redis-backed job storage for the API gateway."""

from __future__ import annotations

import logging
from typing import Any

import redis

from aic_common.contract import JobStatus

from api_gateway.config import Settings


logger = logging.getLogger("api-gateway")


class JobStoreError(Exception):
    """Raised when a job cannot be written to or deleted from Redis."""


class JobStore:
    """Thin field-operation wrapper around an injected Redis client."""

    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    def create_job(self, job_id: str, measurement_id: str) -> None:
        try:
            self.client.hset(
                f"job:{job_id}",
                mapping={
                    "measurement_id": measurement_id,
                    "status": JobStatus.QUEUED.value,
                },
            )
        except redis.exceptions.RedisError as exc:
            raise JobStoreError(f"Could not create job {job_id}") from exc

    def delete_job(self, job_id: str) -> None:
        try:
            self.client.delete(f"job:{job_id}")
        except redis.exceptions.RedisError as exc:
            raise JobStoreError(f"Could not delete job {job_id}") from exc


def create_redis_client(settings: Settings) -> redis.Redis:
    """Create a Redis client with bounded connection and socket timeouts."""
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
