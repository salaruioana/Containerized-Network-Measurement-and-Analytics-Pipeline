"""QoS Analytics Service. See §1.3.3.

Consumes `Queues.ANALYSIS_REQUESTED`. It reads the stored measurement from the
object store, runs the functions in `qos.py`, sets the job to `ANALYZED` and
publishes the result to `Queues.ANALYSIS_COMPLETED`.

It persists nothing. The result becomes durable inside its report, and nowhere
else. On a failed analysis: `FAILED_ANALYSIS`.

Run it with `python -m analytics_service.main`.
"""

from __future__ import annotations

SERVICE_NAME = "analytics-service"


def main() -> None:
    """Connect to the broker and consume `Queues.ANALYSIS_REQUESTED` forever."""
    raise NotImplementedError("analytics_service.main")


if __name__ == "__main__":
    main()
