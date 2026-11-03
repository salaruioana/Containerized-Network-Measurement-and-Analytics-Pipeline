"""Report Service. See §1.3.4.

Consumes `Queues.ANALYSIS_COMPLETED`. It derives the report id from the
measurement id, writes the report with the result embedded to the `reports`
bucket, writes `report_id` to the job and sets the job to `COMPLETED`.

The derived id gives you the idempotency of §1.3.5: a redelivered message
rewrites 1 report object instead of adding a second one. On a failed write:
`FAILED_REPORT_STORAGE`.

Run it with `python -m report_service.main`.
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
    AnalyticsResult,
    Buckets,
    JOB_PREFIX,
    JobStatus,
    Queues,
)
from aic_common.rabbitmq import RabbitMQConsumer
from aic_common.s3 import (
    get_s3_client,
    put_json,
    report_id_for,
    report_key,
)

from aic_common.redis_state import (
    get_redis_client,
    set_job_fields,
    set_job_status,
)

SERVICE_NAME = "report-service"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)


def build_report(job_id: str, measurement_id: str, result: dict) -> dict:
    return {
        "report_id": report_id_for(measurement_id),
        "job_id": job_id,
        "measurement_id": measurement_id,
        "result": result,
    }


def connect_redis() -> redis.Redis:
    """Connect to Redis, retrying while the service is unavailable."""
    client = get_redis_client()

    while True:
        try:
            client.ping()
            logger.info("Connected to Redis")
            return client
        except redis.RedisError:
            logger.warning("Redis unavailable. Retrying in 5 seconds...")
            time.sleep(5)


class ReportStorageError(Exception):
    """Raised when a report cannot be stored in Garage."""


def process_message(payload: dict, redis_client: redis.Redis, s3_client) -> None:
    """Generate and store one report, then update its job in Redis."""
    job_id = payload["job_id"]
    measurement_id = payload["measurement_id"]

    if not isinstance(job_id, str) or not job_id.startswith(JOB_PREFIX) or job_id == JOB_PREFIX:
        raise ValueError("Invalid job_id")

    # The analysis result contains all message fields except job_id.
    result_data = {key: value for key, value in payload.items() if key != "job_id"}

    # Validate the incoming analysis result against the shared contract.
    result = AnalyticsResult.model_validate(result_data)
    if result.measurement_id != measurement_id:
        raise ValueError("Inconsistent measurement_id in analysis result")
    result_dict = result.model_dump(exclude_none=True)

    report = build_report(job_id, measurement_id, result_dict)

    # Store the report using the shared S3 helper. Repeated messages overwrite the same object.
    try:
        put_json(
            bucket=Buckets.REPORTS,
            key=report_key(report["report_id"]),
            data=report,
            client=s3_client,
        )
        logger.info("Stored report %s", report["report_id"])

    except Exception as exc:
        logger.exception("Could not store report %s for job %s", report["report_id"], job_id)

        # Record the failure in Redis before acknowledging the message.
        set_job_status(redis_client, job_id, JobStatus.FAILED_REPORT_STORAGE)
        raise ReportStorageError(f"Could not store report {report['report_id']}") from exc

    # Update only the fields owned by Report Service.
    set_job_fields(
        redis_client,
        job_id,
        {"report_id": report["report_id"]},
    )

    set_job_status(
        redis_client,
        job_id,
        JobStatus.COMPLETED,
    )

    logger.info("Job %s completed", job_id)


def handle_message(
    ch: Any,
    method: Any,
    properties: Any,
    body: bytes,
    redis_client: redis.Redis,
    s3_client: Any,
) -> None:
    """Handle one analysis.completed message with manual ACK/NACK."""
    try:
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("Message payload must be a JSON object")

        logger.info("Received analysis.completed for job %s", payload.get("job_id"))
        process_message(payload, redis_client, s3_client)

    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        logger.error("Invalid analysis.completed message: %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except ReportStorageError:
        # Failure status was recorded in Redis; ACK so message is not redelivered
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logger.exception("Report processing failed")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_COMPLETED` forever."""
    logger.info("Starting %s...", SERVICE_NAME)
    redis_client = connect_redis()
    s3_client = get_s3_client()

    consumer = RabbitMQConsumer(
        queue=Queues.ANALYSIS_COMPLETED,
        handler=lambda ch, method, properties, body: handle_message(
            ch,
            method,
            properties,
            body,
            redis_client=redis_client,
            s3_client=s3_client,
        ),
        prefetch_count=1,
        auto_decode_json=False,
        auto_ack_on_success=False,
        declare_all_queues=True,
    )
    consumer.start(setup_signals=True)
    logger.info("%s stopped cleanly.", SERVICE_NAME)


if __name__ == "__main__":
    main()
