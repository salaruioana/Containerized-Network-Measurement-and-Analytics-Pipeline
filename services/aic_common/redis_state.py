
"""Shared Redis state operations for all services."""

from __future__ import annotations

import os
from typing import Any, Mapping

import redis

from aic_common.contract import (
    JOB_PREFIX,
    MEASUREMENT_PREFIX,
    JobStatus,
)


class RedisStateError(Exception):
    """Raised when a Redis state operation fails."""


def get_redis_client() -> redis.Redis:
    """Create a Redis client using environment configuration."""
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def _job_key(job_id: str) -> str:
    """Build and validate a job Redis key."""
    if (
        not isinstance(job_id, str)
        or not job_id.startswith(JOB_PREFIX)
        or job_id == JOB_PREFIX
    ):
        raise ValueError("Invalid job_id")

    return f"job:{job_id}"


def _measurement_key(measurement_id: str) -> str:
    """Build and validate a measurement Redis key."""
    if (
        not isinstance(measurement_id, str)
        or not measurement_id.startswith(MEASUREMENT_PREFIX)
        or measurement_id == MEASUREMENT_PREFIX
    ):
        raise ValueError("Invalid measurement_id")

    return f"meas:{measurement_id}"


def create_job(
    client: redis.Redis,
    job_id: str,
    measurement_id: str,
) -> None:
    """Create a job with its initial measurement reference and status."""
    key = _job_key(job_id)
    _measurement_key(measurement_id)

    try:
        client.hset(
            key,
            mapping={
                "measurement_id": measurement_id,
                "status": JobStatus.QUEUED.value,
            },
        )
    except redis.RedisError as exc:
        raise RedisStateError(f"Could not create job {job_id}") from exc


def set_job_status(
    client: redis.Redis,
    job_id: str,
    status: JobStatus,
) -> None:
    """Update only the status field of a job."""
    if not isinstance(status, JobStatus):
        raise ValueError("status must be a JobStatus value")

    set_job_fields(
        client,
        job_id,
        {"status": status.value},
    )


def set_job_fields(
    client: redis.Redis,
    job_id: str,
    fields: Mapping[str, Any],
) -> None:
    """Update only the supplied fields of a job."""
    key = _job_key(job_id)

    if not fields:
        raise ValueError("At least one job field must be provided")

    try:
        client.hset(key, mapping=dict(fields))
    except redis.RedisError as exc:
        raise RedisStateError(f"Could not update job {job_id}") from exc


def get_job(
    client: redis.Redis,
    job_id: str,
) -> dict[str, str]:
    """Return all fields of a job. Returns an empty dict if it does not exist."""
    try:
        return client.hgetall(_job_key(job_id))
    except redis.RedisError as exc:
        raise RedisStateError(f"Could not retrieve job {job_id}") from exc


def delete_job(client: redis.Redis, job_id: str) -> None:
    """Delete a job."""
    try:
        client.delete(_job_key(job_id))
    except redis.RedisError as exc:
        raise RedisStateError(f"Could not delete job {job_id}") from exc


def set_measurement_meta(
    client: redis.Redis,
    measurement_id: str,
    metadata: Mapping[str, Any],
) -> None:
    """Create or update measurement metadata fields."""
    key = _measurement_key(measurement_id)

    if not metadata:
        raise ValueError("Measurement metadata cannot be empty")

    try:
        client.hset(key, mapping=dict(metadata))
    except redis.RedisError as exc:
        raise RedisStateError(
            f"Could not update metadata for {measurement_id}"
        ) from exc


def get_measurement_meta(
    client: redis.Redis,
    measurement_id: str,
) -> dict[str, str]:
    """Return measurement metadata. Returns an empty dict if absent."""
    try:
        return client.hgetall(_measurement_key(measurement_id))
    except redis.RedisError as exc:
        raise RedisStateError(
            f"Could not retrieve metadata for {measurement_id}"
        ) from exc


def measurement_exists(
    client: redis.Redis,
    measurement_id: str,
) -> bool:
    """Check whether measurement metadata exists."""
    try:
        return bool(client.exists(_measurement_key(measurement_id)))
    except redis.RedisError as exc:
        raise RedisStateError(
            f"Could not check measurement {measurement_id}"
        ) from exc
