"""Measurement Storage Service. See §1.3.2.

Consumes `Queues.MEASUREMENT_CREATED` and publishes to at most 1 queue. It
writes the raw measurement to the `measurements` bucket, saves the measurement
metadata it owns, and sets the job to `COMPLETED` (Workflow A) or to `STORED`
plus an analytics job on `Queues.ANALYSIS_REQUESTED` (Workflow B).

On a failed write: `FAILED_MEASUREMENT_STORAGE`, and a log line that names the
job.

Run it with `python -m storage_service.main`.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

import pika
import pika.adapters.blocking_connection
import pika.exceptions
import redis
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from aic_common.contract import (
    Buckets,
    JOB_PREFIX,
    JobStatus,
    MEASUREMENT_PREFIX,
    Queues,
)
from aic_common.s3 import (
    get_s3_client,
    measurement_key,
    measurement_object_path,
    put_json,
)

SERVICE_NAME = "storage-service"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)


class StorageMessage(BaseModel):
    """Validated storage request message received on Queues.MEASUREMENT_CREATED."""

    model_config = ConfigDict(extra="ignore")

    job_id: str
    measurement_id: str
    source_host: str
    target_host: str
    metric: str
    values_ms: list[float]
    unit: str = "ms"
    timestamp: str | None = None
    packets_sent: int | None = None
    packets_received: int | None = None
    analyze: bool = False
    analysis: str | None = None
    threshold_ms: int | float | None = 30

    @field_validator("job_id")
    @classmethod
    def validate_job_id(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith(JOB_PREFIX) or v == JOB_PREFIX:
            raise ValueError(f"job_id must start with '{JOB_PREFIX}' and contain an id suffix")
        return v

    @field_validator("measurement_id")
    @classmethod
    def validate_measurement_id(cls, v: str) -> str:
        if not isinstance(v, str) or not v.startswith(MEASUREMENT_PREFIX) or v == MEASUREMENT_PREFIX:
            raise ValueError(
                f"measurement_id must start with '{MEASUREMENT_PREFIX}' and contain an id suffix"
            )
        return v

    @field_validator("source_host", "target_host", "metric")
    @classmethod
    def validate_non_empty_str(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()

    @field_validator("values_ms")
    @classmethod
    def validate_values(cls, v: list[float]) -> list[float]:
        if not v or len(v) == 0:
            raise ValueError("Measurement must contain at least 1 sample in values_ms")
        return [float(x) for x in v]

    @model_validator(mode="before")
    @classmethod
    def normalize_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("Message payload must be a dictionary")

        data = dict(data)

        # Handle nested measurement object if gateway encapsulated it
        if "measurement" in data and isinstance(data["measurement"], dict):
            meas = data.pop("measurement")
            for k, val in meas.items():
                if k not in data:
                    data[k] = val

        # Handle 'values' alias for 'values_ms' per contract §2.1
        if "values_ms" not in data and "values" in data:
            data["values_ms"] = data.pop("values")

        # Set arrival timestamp if missing per contract §2.1
        if not data.get("timestamp"):
            data["timestamp"] = datetime.now(timezone.utc).isoformat()

        # If analysis name provided but analyze flag is false/unset, set analyze=True
        if data.get("analysis") and not data.get("analyze"):
            data["analyze"] = True

        # Default threshold_ms to 30 if unset
        if data.get("threshold_ms") is None:
            data["threshold_ms"] = 30

        return data


def parse_storage_message(raw_payload: str | bytes | dict[str, Any]) -> StorageMessage:
    """Parse and validate incoming storage message.
    
    Raises:
        json.JSONDecodeError: If string/bytes cannot be decoded.
        ValueError / TypeError: If payload violates required fields or contract constraints.
    """
    if isinstance(raw_payload, (str, bytes)):
        data = json.loads(raw_payload)
    elif isinstance(raw_payload, dict):
        data = raw_payload
    else:
        raise TypeError(f"Expected dict, str or bytes, got {type(raw_payload).__name__}")

    return StorageMessage.model_validate(data)


def build_raw_measurement_document(msg: StorageMessage) -> dict[str, Any]:
    """Construct the raw measurement JSON document to be stored in Garage."""
    doc: dict[str, Any] = {
        "measurement_id": msg.measurement_id,
        "source_host": msg.source_host,
        "target_host": msg.target_host,
        "metric": msg.metric,
        "values_ms": msg.values_ms,
        "unit": msg.unit,
        "timestamp": msg.timestamp,
    }
    if msg.packets_sent is not None:
        doc["packets_sent"] = msg.packets_sent
    if msg.packets_received is not None:
        doc["packets_received"] = msg.packets_received
    return doc


def store_raw_measurement(msg: StorageMessage, s3_client: Any = None) -> str:
    """Store the raw measurement JSON in the measurements bucket.
    
    The object key is deterministic: '{source_host}-{target_host}/{measurement_id}.json'.
    Redeliveries safely overwrite the same key without producing duplicate objects.
    Returns the deterministic S3 key.
    """
    if s3_client is None:
        s3_client = get_s3_client()

    key = measurement_key(msg.source_host, msg.target_host, msg.measurement_id)
    doc = build_raw_measurement_document(msg)
    put_json(bucket=Buckets.MEASUREMENTS, key=key, data=doc, client=s3_client)
    logger.info(
        "Stored raw measurement %s at %s/%s (job %s)",
        msg.measurement_id,
        Buckets.MEASUREMENTS,
        key,
        msg.job_id,
    )
    return key


def store_measurement_metadata(msg: StorageMessage, redis_client: Any) -> dict[str, str]:
    """Store measurement metadata hash in Redis at meas:<measurement_id>.
    
    Storage Service is the sole writer of measurement metadata.
    Fields match CONTRACT.md §2.1 and §2.2:
      measurement_id, source_host, target_host, metric, timestamp, object_path, status='stored'.
    """
    obj_path = measurement_object_path(msg.source_host, msg.target_host, msg.measurement_id)
    metadata = {
        "measurement_id": msg.measurement_id,
        "source_host": msg.source_host,
        "target_host": msg.target_host,
        "metric": msg.metric,
        "timestamp": msg.timestamp or "",
        "object_path": obj_path,
        "status": "stored",
    }
    redis_client.hset(name=f"meas:{msg.measurement_id}", mapping=metadata)
    logger.info(
        "Saved metadata in Redis for %s at meas:%s (job %s)",
        msg.measurement_id,
        msg.measurement_id,
        msg.job_id,
    )
    return metadata


class StorageWriteError(Exception):
    """Raised when writing raw measurement to S3 or metadata to Redis fails."""
    pass


def complete_job_workflow_a(job_id: str, redis_client: Any) -> None:
    """Mark job as completed in Redis for Workflow A (store-only).
    
    Per CONTRACT.md §5, Task #21, and Task #24:
    - Updates single field 'status' to 'completed' without read-modify-write.
    - Idempotent: if already completed, leaves it intact.
    - Does not regress terminal failure statuses.
    """
    current_status = redis_client.hget(f"job:{job_id}", "status")
    if current_status == JobStatus.COMPLETED.value:
        logger.info("Job %s is already %s (idempotent redelivery)", job_id, current_status)
        return
    if current_status in (
        JobStatus.FAILED_MEASUREMENT_STORAGE.value,
        JobStatus.FAILED_ANALYSIS.value,
        JobStatus.FAILED_REPORT_STORAGE.value,
    ):
        logger.warning(
            "Job %s is in terminal failure '%s'; skipping transition to %s",
            job_id,
            current_status,
            JobStatus.COMPLETED.value,
        )
        return

    redis_client.hset(name=f"job:{job_id}", key="status", value=JobStatus.COMPLETED.value)
    logger.info("Marked job %s as %s", job_id, JobStatus.COMPLETED.value)


def handle_storage_failure(
    job_id: str,
    exc: Exception,
    redis_client: Any = None,
    channel: Any = None,
    measurement_id: str | None = None,
) -> None:
    """Handle S3 or Redis storage write failure.
    
    Per CONTRACT.md §2.2, §6 and Task #23:
    1. Sets job status in Redis to failed_measurement_storage (single-field update).
    2. Logs an error line explicitly naming the job_id.
    3. Optionally publishes to Queues.MEASUREMENT_FAILED.
    """
    logger.error("Failed measurement storage for job %s: %s", job_id, exc)
    if redis_client is not None:
        try:
            redis_client.hset(
                name=f"job:{job_id}",
                key="status",
                value=JobStatus.FAILED_MEASUREMENT_STORAGE.value,
            )
            logger.info(
                "Marked job %s as %s", job_id, JobStatus.FAILED_MEASUREMENT_STORAGE.value
            )
        except Exception as redis_exc:
            logger.error(
                "Could not update failure status in Redis for job %s: %s", job_id, redis_exc
            )

    if channel is not None:
        try:
            channel.queue_declare(queue=Queues.MEASUREMENT_FAILED, durable=True)
            err_payload = {
                "job_id": job_id,
                "measurement_id": measurement_id or "",
                "status": JobStatus.FAILED_MEASUREMENT_STORAGE.value,
                "error": str(exc),
            }
            channel.basic_publish(
                exchange="",
                routing_key=Queues.MEASUREMENT_FAILED,
                body=json.dumps(err_payload).encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )
        except Exception as pub_exc:
            logger.debug("Optional publish to %s failed: %s", Queues.MEASUREMENT_FAILED, pub_exc)


def request_analysis_workflow_b(
    msg: StorageMessage,
    redis_client: Any,
    channel: pika.adapters.blocking_connection.BlockingChannel | Any = None,
) -> dict[str, Any]:
    """Advance job to 'stored' in Redis and publish to measurement.analysis.requested.
    
    Per CONTRACT.md §2.2, §3, §5, Task #22, and Task #24:
    1. Sets job status to 'stored' in Redis via single-field HSET (no read-modify-write).
    2. Prevents status regression: a redelivery must never move a job backwards
       (e.g. from 'analyzed' or 'completed' back to 'stored').
    3. Prevents duplicate publishing if job has already progressed past 'stored'.
    4. Publishes {job_id, measurement_id, analysis, threshold_ms} to Queues.ANALYSIS_REQUESTED.
    5. Uses delivery_mode=2 (Persistent) and declares queue durable.
    6. Exactly one queue is published to (one queue in, one queue out).
    """
    payload = {
        "job_id": msg.job_id,
        "measurement_id": msg.measurement_id,
        "analysis": msg.analysis or "latency_summary",
        "threshold_ms": msg.threshold_ms if msg.threshold_ms is not None else 30,
    }

    current_status = redis_client.hget(f"job:{msg.job_id}", "status") if redis_client is not None else None

    # Guard against status regression and duplicate downstream dispatch on redelivery (Task #24)
    if current_status in (
        JobStatus.ANALYZED.value,
        JobStatus.COMPLETED.value,
        JobStatus.FAILED_ANALYSIS.value,
        JobStatus.FAILED_REPORT_STORAGE.value,
    ):
        logger.info(
            "Job %s has already reached '%s'; skipping regression to '%s' and duplicate publish.",
            msg.job_id,
            current_status,
            JobStatus.STORED.value,
        )
        return payload

    if current_status != JobStatus.STORED.value:
        redis_client.hset(name=f"job:{msg.job_id}", key="status", value=JobStatus.STORED.value)
        logger.info("Marked job %s as %s", msg.job_id, JobStatus.STORED.value)
    else:
        logger.info(
            "Job %s is already %s (redelivery before downstream processing)",
            msg.job_id,
            current_status,
        )

    if channel is not None:
        channel.queue_declare(queue=Queues.ANALYSIS_REQUESTED, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=Queues.ANALYSIS_REQUESTED,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        logger.info(
            "Published analysis request for job %s (measurement %s) to %s",
            msg.job_id,
            msg.measurement_id,
            Queues.ANALYSIS_REQUESTED,
        )

    return payload


def process_message(
    msg: StorageMessage,
    s3_client: Any = None,
    redis_client: Any = None,
    channel: Any = None,
) -> None:
    """Process a single storage job.
    
    1. Write the raw measurement to Garage S3 before metadata/job status (Task #19).
    2. Write measurement metadata to Redis (Task #20).
    3. On storage write failure: set status=failed_measurement_storage and log job_id (Task #23).
    4. Update job status in Redis & optionally forward to analytics (Tasks #21 / #22).
    """
    logger.info("Consumed storage job %s for measurement %s", msg.job_id, msg.measurement_id)
    try:
        store_raw_measurement(msg, s3_client=s3_client)
        if redis_client is not None:
            store_measurement_metadata(msg, redis_client=redis_client)
    except Exception as exc:
        handle_storage_failure(
            job_id=msg.job_id,
            exc=exc,
            redis_client=redis_client,
            channel=channel,
            measurement_id=msg.measurement_id,
        )
        raise StorageWriteError(f"Storage write failed for job {msg.job_id}") from exc

    if redis_client is not None:
        if not msg.analyze:
            complete_job_workflow_a(msg.job_id, redis_client=redis_client)
        else:
            request_analysis_workflow_b(msg, redis_client=redis_client, channel=channel)


def handle_message(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    properties: pika.spec.BasicProperties,
    body: bytes,
    s3_client: Any = None,
    redis_client: Any = None,
) -> None:
    """Callback handler for measurement.created messages with manual ACK/NACK."""
    try:
        msg = parse_storage_message(body)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        logger.error(
            "Rejected malformed message on %s (requeue=False): %s | Raw body: %s",
            Queues.MEASUREMENT_CREATED,
            exc,
            body[:200] if isinstance(body, (bytes, str)) else body,
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        process_message(msg, s3_client=s3_client, redis_client=redis_client, channel=ch)
    except StorageWriteError as exc:
        logger.error("Job %s terminated with failure during storage: %s", msg.job_id, exc)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    except Exception as exc:
        logger.exception("Unexpected error processing storage message: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # Drill hook for testing unacknowledged crash and restart (Task #24 / #54):
    drill_job = os.getenv("DRILL_CRASH_BEFORE_ACK")
    if drill_job and (drill_job.lower() in ("true", "1") or drill_job == msg.job_id) and not method.redelivered:
        logger.warning(
            "DRILL: Simulating container crash before ACK for job %s (exiting without ACK)",
            msg.job_id,
        )
        os._exit(42)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_rabbitmq(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    retry_interval: float = 5.0,
) -> pika.BlockingConnection:
    """Connect to RabbitMQ, retrying while the broker is unavailable."""
    host = host or os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = port or int(os.getenv("RABBITMQ_PORT", "5672"))
    user = user or os.getenv("RABBITMQ_USER", "guest")
    password = password or os.getenv("RABBITMQ_PASSWORD", "guest")

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            logger.info("Connected to RabbitMQ at %s:%s", host, port)
            return connection
        except (pika.exceptions.AMQPConnectionError, OSError) as exc:
            logger.warning(
                "RabbitMQ at %s:%s unavailable (%s). Retrying in %.0f seconds...",
                host,
                port,
                exc,
                retry_interval,
            )
            time.sleep(retry_interval)


def connect_redis(
    host: str | None = None,
    port: int | None = None,
    retry_interval: float = 5.0,
) -> redis.Redis:
    """Connect to Redis, retrying while the service is unavailable."""
    host = host or os.getenv("REDIS_HOST", "redis")
    port = port or int(os.getenv("REDIS_PORT", "6379"))

    client = redis.Redis(
        host=host,
        port=port,
        decode_responses=True,
    )

    while True:
        try:
            client.ping()
            logger.info("Connected to Redis at %s:%s", host, port)
            return client
        except (redis.RedisError, OSError) as exc:
            logger.warning(
                "Redis at %s:%s unavailable (%s). Retrying in %.0f seconds...",
                host,
                port,
                exc,
                retry_interval,
            )
            time.sleep(retry_interval)


class ConsumerContext:
    """Manages consumer connection and graceful shutdown state."""

    def __init__(self) -> None:
        self.stop_requested = False
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def request_stop(self) -> None:
        self.stop_requested = True
        if self.channel and self.channel.is_open:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass


def main() -> None:
    """Connect to the broker and consume `Queues.MEASUREMENT_CREATED` forever."""
    context = ConsumerContext()

    def handle_signal(signum: int, frame: Any) -> None:
        logger.info("Received signal %s. Initiating graceful shutdown...", signum)
        context.request_stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, handle_signal)
        except (ValueError, AttributeError) as exc:
            logger.debug("Could not register signal %s: %s", sig, exc)

    logger.info("Starting %s...", SERVICE_NAME)
    s3_client = get_s3_client()
    redis_client = connect_redis()

    while not context.stop_requested:
        try:
            connection = connect_rabbitmq()
            context.connection = connection
            channel = connection.channel()
            context.channel = channel

            # Declare durable queues
            channel.queue_declare(
                queue=Queues.MEASUREMENT_CREATED,
                durable=True,
            )
            channel.queue_declare(
                queue=Queues.ANALYSIS_REQUESTED,
                durable=True,
            )
            channel.queue_declare(
                queue=Queues.MEASUREMENT_FAILED,
                durable=True,
            )

            # Fair dispatch: prefetch count = 1
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                handle_message(
                    ch,
                    method,
                    properties,
                    body,
                    s3_client=s3_client,
                    redis_client=redis_client,
                )

            channel.basic_consume(
                queue=Queues.MEASUREMENT_CREATED,
                on_message_callback=callback,
                auto_ack=False,
            )

            logger.info("Waiting for %s messages...", Queues.MEASUREMENT_CREATED)
            channel.start_consuming()

        except (pika.exceptions.AMQPError, OSError) as exc:
            if context.stop_requested:
                break
            logger.exception("RabbitMQ connection dropped (%s). Reconnecting in 5s...", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping...")
            break
        finally:
            if context.connection and context.connection.is_open:
                try:
                    context.connection.close()
                except Exception:
                    pass

    logger.info("%s stopped cleanly.", SERVICE_NAME)


if __name__ == "__main__":
    main()
