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

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api_gateway.models import AnalyzeStoredIn, MeasurementIn, SubmitAnalyzeIn
from api_gateway.config import Settings, load_settings
from api_gateway.publisher import PublishError, Publisher
from api_gateway.store import JobStore, JobStoreError, create_redis_client


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


def _validation_summary(exc: RequestValidationError) -> str:
    first_error = exc.errors()[0]
    location = first_error.get("loc", ())
    field = str(location[-1]) if location else "request"
    if first_error.get("type") == "json_invalid":
        return "invalid JSON"
    if first_error.get("type") == "missing":
        return f"{field}: Field required"
    return f"{field}: invalid value"


@app.exception_handler(RequestValidationError)
async def request_validation_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": _validation_summary(exc)})


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


@app.exception_handler(PublishError)
async def publish_error_handler(request: Request, exc: PublishError) -> JSONResponse:
    logger.exception("Message broker unavailable", exc_info=exc)
    return JSONResponse(status_code=503, content={"error": "message broker unavailable"})


@app.exception_handler(JobStoreError)
async def job_store_error_handler(request: Request, exc: JobStoreError) -> JSONResponse:
    logger.exception("Job store unavailable", exc_info=exc)
    return JSONResponse(status_code=503, content={"error": "job store unavailable"})


def get_settings(request: Request) -> Settings:
    if not hasattr(request.app.state, "settings"):
        request.app.state.settings = load_settings()
    return request.app.state.settings


def get_job_store(request: Request) -> JobStore:
    if not hasattr(request.app.state, "job_store"):
        request.app.state.job_store = JobStore(create_redis_client(get_settings(request)))
    return request.app.state.job_store


def get_publisher(request: Request) -> Publisher:
    if not hasattr(request.app.state, "publisher"):
        request.app.state.publisher = Publisher(get_settings(request))
    return request.app.state.publisher


@app.post("/measurements", status_code=202)
def submit_measurement(body: MeasurementIn) -> None:
    raise NotImplementedError("POST /measurements")


@app.post("/measurements/analyze", status_code=202)
def submit_and_analyze(body: SubmitAnalyzeIn) -> None:
    raise NotImplementedError("POST /measurements/analyze")


@app.post("/measurements/{measurement_id}/analyze", status_code=202)
def analyze_stored_measurement(
    measurement_id: str,
    body: AnalyzeStoredIn,
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
