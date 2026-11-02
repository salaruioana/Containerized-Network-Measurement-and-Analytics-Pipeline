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

import pika
import redis

from aic_common.contract import (
    AnalyticsResult,
    Buckets,
    JOB_PREFIX,
    JobStatus,
    Queues,
)
from aic_common.s3 import (
    get_s3_client,
    put_json,
    report_id_for,
    report_key,
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
        redis_client.hset(f"job:{job_id}", mapping={"status": JobStatus.FAILED_REPORT_STORAGE.value})

        raise ReportStorageError(f"Could not store report {report['report_id']}") from exc

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
    redis_client = connect_redis()
    s3_client = get_s3_client()

    while True:
        connection = None

        try:
            connection = connect_rabbitmq()
            channel = connection.channel()

            # The consumer declares the queue as durable.
            channel.queue_declare(
                queue=Queues.ANALYSIS_COMPLETED,
                durable=True,
            )

            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body)

                    if not isinstance(payload, dict):
                        raise ValueError("Message payload must be a JSON object")
                    
                    logger.info(
                            "Received analysis.completed for job %s",
                            payload.get("job_id"),
                        )

                    process_message(payload, redis_client, s3_client)

                except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
                    logger.error("Invalid analysis.completed message: %s", exc)
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False,
                    )

                except ReportStorageError:
                    # The failure status was successfully saved in Redis.
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception:
                    logger.exception("Report processing failed")
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True,
                    )

                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=Queues.ANALYSIS_COMPLETED,
                on_message_callback=callback,
                auto_ack=False,
            )

            logger.info("Waiting for analysis.completed messages")
            channel.start_consuming()

        except (pika.exceptions.AMQPError, OSError):
            logger.exception("RabbitMQ connection lost. Reconnecting...")
            time.sleep(5)

        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
