"""The fixed names of the contract, in one place.

Everything in this module is fixed by CONTRACT.md. It holds names and no logic,
so a typo cannot cost you a passing test. Everything else is your design:
the messages on the queues, the object keys, the Redis layout, and the classes
that read and write them.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict

# Id prefixes. A report id is derived from its measurement id, so `m-a1b2`
# always gives `r-a1b2`.
MEASUREMENT_PREFIX = "m-"
JOB_PREFIX = "job-"
REPORT_PREFIX = "r-"


class Queues:
    """The RabbitMQ queues. Each one is named for the event that puts work on it.

    `ANALYSIS_REQUESTED` has 2 producers: the storage service after a write that
    also wants analysis, and the gateway for a measurement stored earlier.
    """

    MEASUREMENT_CREATED = "measurement.created"
    ANALYSIS_REQUESTED = "measurement.analysis.requested"
    ANALYSIS_COMPLETED = "analysis.completed"
    # Optional. Nothing consumes it.
    MEASUREMENT_FAILED = "measurement.failed"


class Buckets:
    """The object store buckets. Each one has exactly 1 writer."""

    MEASUREMENTS = "measurements"
    REPORTS = "reports"
    # Optional.
    ERRORS = "errors"


class JobStatus(str, Enum):
    """The job states. Each failure names the stage that failed, so a job that
    stops advancing says where it stopped.
    """

    QUEUED = "queued"
    STORED = "stored"
    ANALYZED = "analyzed"
    COMPLETED = "completed"
    FAILED_MEASUREMENT_STORAGE = "failed_measurement_storage"
    FAILED_ANALYSIS = "failed_analysis"
    FAILED_REPORT_STORAGE = "failed_report_storage"


class AnalyticsResult(BaseModel):
    """The analysis, as it appears in a job response and in a report.

    The field names are the wire names. A measurement carries 1 metric, so fill
    in only the fields of that metric and leave the rest None. Serialize with
    `model_dump(exclude_none=True)` to drop them.

    Required: `measurement_id`, `metric`, `sample_count`.
    """

    model_config = ConfigDict(extra="forbid")

    measurement_id: str
    metric: str
    unit: str | None = None
    sample_count: int = 0
    # metric=latency
    average_latency_ms: float | None = None
    min_latency_ms: int | None = None
    max_latency_ms: int | None = None
    # metric=latency (successive differences) and metric=jitter (the samples are
    # jitter values already, so they are averaged)
    jitter_ms: float | None = None
    # metric=throughput
    throughput_avg_mbps: float | None = None
    throughput_min_mbps: float | None = None
    throughput_max_mbps: float | None = None
    # Only when both packet counts arrived.
    packet_loss_percent: float | None = None
    # The threshold the result was judged against.
    threshold_ms: int | None = None
    anomaly_detected: bool = False
    quality: str = "nominal"
