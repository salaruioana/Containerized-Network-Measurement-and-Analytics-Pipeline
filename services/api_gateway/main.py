"""Measurement API Gateway. See §1.3.1.

The only service that speaks HTTP. It validates a request, mints the ids,
records the job in Redis and publishes to RabbitMQ. It never writes a
measurement, an analysis or a report, and it never waits for one: every POST
answers as soon as its message is on a queue.

Only `/health` is here. The 6 endpoints of the contract are yours to add:

    POST /measurements
    POST /measurements/analyze
    POST /measurements/{measurement_id}/analyze
    GET  /jobs/{job_id}
    GET  /measurements/{measurement_id}
    GET  /reports/{report_id}

Run it with `uvicorn api_gateway.main:app --host 0.0.0.0 --port 8080`.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.responses import JSONResponse


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("api-gateway")

SERVICE_NAME = "api-gateway"

app = FastAPI(title="Measurement API Gateway")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "UP", "service": SERVICE_NAME}


@app.exception_handler(NotImplementedError)
async def not_implemented_handler(
    request: Request, exc: NotImplementedError
) -> JSONResponse:
    logger.warning("Gateway route is not implemented: %s", exc)
    return JSONResponse(status_code=501, content={"error": "not implemented"})


@app.post("/measurements", status_code=202)
def submit_measurement(body: dict[str, Any] = Body(default_factory=dict)) -> None:
    raise NotImplementedError("POST /measurements")


@app.post("/measurements/analyze", status_code=202)
def submit_and_analyze(body: dict[str, Any] = Body(default_factory=dict)) -> None:
    raise NotImplementedError("POST /measurements/analyze")


@app.post("/measurements/{measurement_id}/analyze", status_code=202)
def analyze_stored_measurement(
    measurement_id: str,
    body: dict[str, Any] = Body(default_factory=dict),
) -> None:
    raise NotImplementedError("POST /measurements/{measurement_id}/analyze")


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> None:
    raise NotImplementedError("GET /jobs/{job_id}")


@app.get("/measurements/{measurement_id}")
def get_measurement(measurement_id: str) -> None:
    raise NotImplementedError("GET /measurements/{measurement_id}")


@app.get("/reports/{report_id}")
def get_report(
    report_id: str,
    fmt: str | None = Query(default=None, alias="format"),
) -> None:
    raise NotImplementedError("GET /reports/{report_id}")
