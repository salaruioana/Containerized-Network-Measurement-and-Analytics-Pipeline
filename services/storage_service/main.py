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

SERVICE_NAME = "storage-service"


def main() -> None:
    """Connect to the broker and consume `Queues.MEASUREMENT_CREATED` forever."""
    raise NotImplementedError("storage_service.main")


if __name__ == "__main__":
    main()
