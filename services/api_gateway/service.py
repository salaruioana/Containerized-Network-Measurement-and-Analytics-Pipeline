"""Job creation and publish ordering for gateway workflows."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from aic_common.contract import JOB_PREFIX, MEASUREMENT_PREFIX

from api_gateway.publisher import PublishError
from api_gateway.store import JobStoreError


logger = logging.getLogger("api-gateway")


class SubmitResponse(BaseModel):
    job_id: str
    measurement_id: str
    status: str = "queued"


def create_job_and_publish(
    store: Any,
    publisher: Any,
    queue: str,
    build_message: Callable[[str, str], dict[str, Any]],
    measurement_id: str | None = None,
) -> SubmitResponse:
    """Create a queued job, publish its message, and roll back publish failures."""
    job_id = f"{JOB_PREFIX}{uuid4().hex}"
    resolved_measurement_id = (
        measurement_id
        if measurement_id is not None
        else f"{MEASUREMENT_PREFIX}{uuid4().hex}"
    )

    store.create_job(job_id, resolved_measurement_id)
    message = build_message(job_id, resolved_measurement_id)

    try:
        publisher.publish(queue, message)
    except PublishError:
        try:
            store.delete_job(job_id)
        except Exception:
            logger.exception("Could not roll back job %s", job_id)
        logger.warning(
            "Rolled back job %s for measurement %s",
            job_id,
            resolved_measurement_id,
        )
        raise

    logger.info(
        "Accepted job %s for measurement %s",
        job_id,
        resolved_measurement_id,
    )
    return SubmitResponse(
        job_id=job_id,
        measurement_id=resolved_measurement_id,
        status="queued",
    )
