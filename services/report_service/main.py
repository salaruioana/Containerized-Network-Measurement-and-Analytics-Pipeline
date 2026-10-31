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
from aic_common.contract import MEASUREMENT_PREFIX, REPORT_PREFIX

SERVICE_NAME = "report-service"

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


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_COMPLETED` forever."""
    raise NotImplementedError("report_service.main")


if __name__ == "__main__":
    main()
