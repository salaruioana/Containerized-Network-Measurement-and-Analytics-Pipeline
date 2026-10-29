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

from fastapi import FastAPI

SERVICE_NAME = "api-gateway"

app = FastAPI(title="Measurement API Gateway")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "UP", "service": SERVICE_NAME}
