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

SERVICE_NAME = "report-service"


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_COMPLETED` forever."""
    raise NotImplementedError("report_service.main")


if __name__ == "__main__":
    main()
