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

import boto3
import pika
import redis


from aic_common.contract import (
    AnalyticsResult,
    Buckets,
    JOB_PREFIX,
    JobStatus,
    MEASUREMENT_PREFIX,
    Queues,
    REPORT_PREFIX,
)

SERVICE_NAME = "report-service"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

def report_id_for(measurement_id: str) -> str:
    if not measurement_id.startswith(MEASUREMENT_PREFIX):
        raise ValueError("Invalid measurement_id")
    
    suffix = measurement_id[len(MEASUREMENT_PREFIX):]
    if not suffix:
        raise ValueError("Invalid measurement_id")
    
    return f"{REPORT_PREFIX}{suffix}"

def build_report(job_id: str, measurement_id: str, result: dict) -> dict:
    return {
        "report_id": report_id_for(measurement_id),
        "job_id":job_id,
        "measurement_id":measurement_id,
        "result":result
    }
    
def connect_redis() -> redis.Redis:
    """Connect to Redis, retrying while the service is unavailable."""
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
        except redis.RedisError:
            logger.warning("Redis unavailable. Retrying in 5 seconds...")
            time.sleep(5)

def create_s3_client():
    """Create an S3 client configured for Garage."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.getenv("S3_REGION", "garage"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )
    
def process_message(
    payload: dict,
    redis_client: redis.Redis,
    s3_client,
) -> None:
    """Generate and store one report, then update its job in Redis."""

    job_id = payload["job_id"]
    measurement_id = payload["measurement_id"]

    if not job_id.startswith(JOB_PREFIX) or job_id == JOB_PREFIX:
        raise ValueError("Invalid job_id")

    # The analysis result contains all message fields except job_id.
    result_data = {
        key: value
        for key, value in payload.items()
        if key != "job_id"
    }

    # Validate the incoming analysis result against the shared contract.
    result = AnalyticsResult.model_validate(result_data)
    result_dict = result.model_dump(exclude_none=True)

    report = build_report(job_id, measurement_id, result_dict)

    # Store the report. Repeated messages overwrite the same object.
    try:
        s3_client.put_object(
            Bucket=Buckets.REPORTS,
            Key=report["report_id"],
            Body=json.dumps(report, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Stored report %s", report["report_id"])

    except Exception:
        logger.exception("Could not store report %s", report["report_id"])

        # Report storage failed: update the job status.
        redis_client.hset(
            f"job:{job_id}",
            mapping={
                "status": JobStatus.FAILED_REPORT_STORAGE.value,
            },
        )
        raise

    # Update only the fields owned by Report Service.
    redis_client.hset(
        f"job:{job_id}",
        mapping={
            "report_id": report["report_id"],
            "status": JobStatus.COMPLETED.value,
        },
    )

    logger.info("Job %s completed", job_id)

def connect_rabbitmq() -> pika.BlockingConnection:
    """Connect to RabbitMQ, retrying while the broker is unavailable."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASSWORD", "guest"),
    )

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                    port=int(os.getenv("RABBITMQ_PORT", "5672")),
                    credentials=credentials,
                    heartbeat=60,
                    blocked_connection_timeout=30,
                )
            )
            logger.info("Connected to RabbitMQ")
            return connection

        except (pika.exceptions.AMQPConnectionError, OSError):
            logger.warning("RabbitMQ unavailable. Retrying in 5 seconds...")
            time.sleep(5)


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_COMPLETED` forever."""
    raise NotImplementedError("report_service.main")


if __name__ == "__main__":
    main()
