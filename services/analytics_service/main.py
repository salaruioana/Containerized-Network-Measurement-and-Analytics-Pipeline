"""QoS Analytics Service. See §1.3.3.

Consumes `Queues.ANALYSIS_REQUESTED`. It reads the stored measurement from the
object store, runs the functions in `qos.py`, sets the job to `ANALYZED` and
publishes the result to `Queues.ANALYSIS_COMPLETED`.

It persists nothing. The result becomes durable inside its report, and nowhere
else. On a failed analysis: `FAILED_ANALYSIS`.

Run it with `python -m analytics_service.main`.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import pika
import redis

from aic_common.contract import (
    JOB_PREFIX,
    MEASUREMENT_PREFIX,
    AnalyticsResult,
    Buckets,
    JobStatus,
    Queues,
)
from aic_common.rabbitmq import RabbitMQConsumer
from aic_common.redis_state import (
    InvalidJobTransition,
    get_measurement_meta,
    set_job_status,
)
from aic_common.s3 import S3ObjectNotFoundError, get_json, get_s3_client, key_from_object_path
from analytics_service import qos

SERVICE_NAME = "analytics-service"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)


class MalformedRequestError(Exception):
    """The message itself is broken. No usable job_id, so no job to mark."""


class AnalysisError(Exception):
    """The job exists but cannot be analysed. Retrying will not help."""


# Step 1: read the request and the measurement

def parse_request(payload: Any) -> dict[str, Any]:
    """Check the analysis request and return its fields."""
    if not isinstance(payload, dict):
        raise MalformedRequestError("message must be a JSON object")

    job_id = payload.get("job_id")
    measurement_id = payload.get("measurement_id")
    if not isinstance(job_id, str) or not job_id.startswith(JOB_PREFIX):
        raise MalformedRequestError(f"invalid job_id: {job_id!r}")
    if not isinstance(measurement_id, str) or not measurement_id.startswith(MEASUREMENT_PREFIX):
        raise MalformedRequestError(f"invalid measurement_id: {measurement_id!r}")

    return {
        "job_id": job_id,
        "measurement_id": measurement_id,
        "analysis": payload.get("analysis"),
        "threshold_ms": payload.get("threshold_ms"),
    }


def load_measurement(measurement_id: str, redis_client: Any, s3_client: Any) -> dict[str, Any]:
    """Find the raw measurement via its Redis metadata and read it from Garage."""
    meta = get_measurement_meta(redis_client, measurement_id)
    object_path = meta.get("object_path")
    if not object_path:
        raise AnalysisError(f"no metadata for measurement {measurement_id}")

    try:
        key = key_from_object_path(object_path, expected_bucket=Buckets.MEASUREMENTS)
        return get_json(Buckets.MEASUREMENTS, key, client=s3_client)
    except (S3ObjectNotFoundError, ValueError) as exc:
        raise AnalysisError(f"cannot read measurement {measurement_id}: {exc}") from exc


# Step 2: analyse, set the status, publish

def run_analysis(request: dict[str, Any], raw: dict[str, Any]) -> AnalyticsResult:
    """Run qos.analyze() on the stored samples with the requested settings."""
    try:
        return qos.analyze(
            measurement_id=request["measurement_id"],
            analysis=request["analysis"],
            metric=raw.get("metric"),
            values=raw.get("values_ms"),
            unit=raw.get("unit"),
            packets_sent=raw.get("packets_sent"),
            packets_received=raw.get("packets_received"),
            threshold_ms=request["threshold_ms"],
        )
    except (ValueError, TypeError) as exc:
        raise AnalysisError(f"analysis failed for {request['measurement_id']}: {exc}") from exc


def build_completed_message(job_id: str, result: AnalyticsResult) -> dict[str, Any]:
    """The agreed flat format: job_id plus the result fields, no nulls."""
    return {"job_id": job_id, **result.model_dump(exclude_none=True)}


def publish_completed(channel: Any, message: dict[str, Any]) -> None:
    """Publish the result to analysis.completed as a persistent message."""
    channel.queue_declare(queue=Queues.ANALYSIS_COMPLETED, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=Queues.ANALYSIS_COMPLETED,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )


def mark_failed(redis_client: Any, job_id: str, reason: str) -> None:
    """Set failed_analysis, unless the job has already moved on."""
    logger.error("Analysis failed for job %s: %s", job_id, reason)
    try:
        set_job_status(redis_client, job_id, JobStatus.FAILED_ANALYSIS)
    except InvalidJobTransition:
        logger.warning("Job %s is past analysis, failure not recorded", job_id)


def process_message(payload: Any, redis_client: Any, s3_client: Any, channel: Any) -> dict[str, Any] | None:
    """Handle one analysis request. Returns the published message, or None if skipped."""
    request = parse_request(payload)
    job_id = request["job_id"]

    try:
        raw = load_measurement(request["measurement_id"], redis_client, s3_client)
        result = run_analysis(request, raw)
    except AnalysisError as exc:
        mark_failed(redis_client, job_id, str(exc))
        raise

    # First status, then publish. A fast Report Service must not be
    # overwritten back to 'analyzed' after it already set 'completed'.
    try:
        set_job_status(redis_client, job_id, JobStatus.ANALYZED)
    except InvalidJobTransition:
        # Redelivery of a job that is already completed, return none.
        logger.info("Job %s is already past analysis, skipping redelivered message", job_id)
        return None

    message = build_completed_message(job_id, result)
    publish_completed(channel, message)
    logger.info("Job %s analyzed (%s), result published", job_id, result.quality)
    return message


# Step 3: ack or nack


def handle_message(ch: Any, method: Any, properties: Any, body: bytes,
                   redis_client: Any, s3_client: Any) -> None:
    """Process one message and decide: ack, drop, or retry later."""
    try:
        process_message(json.loads(body), redis_client, s3_client, ch)
    except (json.JSONDecodeError, MalformedRequestError) as exc:
        logger.error("Dropping malformed analysis request: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except AnalysisError:
        # failed_analysis is recorded, retrying would fail the same way.
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        # Redis or RabbitMQ trouble, may work later, so put it back.
        logger.exception("Temporary error, requeueing analysis request")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


# Step 4: start the service


def connect_redis(retry_interval: float = 5.0) -> redis.Redis:
    """Connect to Redis, retrying until it is reachable."""
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    while True:
        try:
            client.ping()
            logger.info("Connected to Redis")
            return client
        except (redis.RedisError, OSError):
            logger.warning("Redis unavailable, retrying in %.0f s", retry_interval)
            time.sleep(retry_interval)


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_REQUESTED` forever."""
    logger.info("Starting %s...", SERVICE_NAME)
    redis_client = connect_redis()
    s3_client = get_s3_client()

    consumer = RabbitMQConsumer(
        queue=Queues.ANALYSIS_REQUESTED,
        handler=lambda ch, method, properties, body: handle_message(
            ch, method, properties, body,
            redis_client=redis_client,
            s3_client=s3_client,
        ),
        # one message at a time, so 2 replicas share the queue evenly
        prefetch_count=1,
        auto_decode_json=False,
        auto_ack_on_success=False,
        declare_all_queues=True,
    )
    consumer.start(setup_signals=True)
    logger.info("%s stopped cleanly.", SERVICE_NAME)


if __name__ == "__main__":
    main()