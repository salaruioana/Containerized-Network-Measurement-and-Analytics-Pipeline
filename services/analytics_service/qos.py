"""The QoS statistics. See §1.3.3.

Pure computation, which is why this is the one module with a unit test written
for you. `tests/../tests/test_qos.py` is red and is your specification: every
expected number in it comes from a worked example in the assignment.

§1.3.3 asks for at least 2 analytics functions, not all of them. A field left
None is dropped from the result and skipped by the tests. Whatever you do
implement must be correct.
"""

from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP

from aic_common.contract import AnalyticsResult

DEFAULT_THRESHOLD_MS = 30.0
# Packet loss has its own ceiling. Loss and delay are different failure modes.
LOSS_ANOMALY_PERCENT = 5.0
# A batch at 60% of the ceiling still has a third of its budget left.
NOMINAL_HEADROOM_RATIO = 0.6
# Metrics measured in milliseconds and judged against threshold_ms.
DELAY_METRICS = ("latency", "jitter")


def round_half_up(value: float, digits: int = 1) -> float:
    """Round half away from zero.

    :param value: the number to round.
    :param digits: decimal places, 0 for whole numbers.
    :return: the rounded number.

    Note: `round(17.45, 1)` in Python gives 17.4, not 17.5.
    """
    d = Decimal(str(value))
    step = Decimal(1).scaleb(-digits)
    return float(d.quantize(step, rounding=ROUND_HALF_UP))


def average(values: list[float]) -> float:
    """Arithmetic mean of the samples.

    :param values: at least 1 sample.
    :return: the mean, rounded to 1 decimal.
    """
    return round_half_up(sum(values) / len(values), 1)


def jitter(values: list[float]) -> float:
    """Mean absolute difference between consecutive samples.

    :param values: at least 1 sample.
    :return: the mean of `abs(v[i] - v[i-1])`, rounded to 1 decimal.

    This is not variance. `[10, 30, 10, 30, 10]` and `[10, 10, 10, 30, 30]` have
    the same mean and the same spread, but the first swings on every step and
    the second drifts once. A call stutters when the delay changes between 2
    packets, not when the average is high. Decide what 1 sample gives you.
    """
    if len(values) < 2:
        return 0.0
    diffs = [abs(cur - prev) for prev, cur in zip(values, values[1:])]
    return round_half_up(sum(diffs) / len(diffs), 1)


def packet_loss_percent(sent: int | None, received: int | None) -> float | None:
    """Loss percentage from the 2 packet counts.

    :param sent: packets sent, or None if the client did not say.
    :param received: packets received, or None if the client did not say.
    :return: `(sent - min(received, sent)) / sent * 100`, or None if a count is
        missing.

    A missing count is not zero loss: "we do not know" and "none was lost" are
    different claims. More received than sent is 0.0, never a negative number.
    """


def resolve_metric(analysis: str | None, metric: str | None) -> str:
    """Decide which metric to compute.

    :param analysis: the requested analysis, for example `latency_summary`.
    :param metric: the metric of the measurement, used as the fallback.
    :return: the metric name, for example `latency`.
    """
    raise NotImplementedError("qos.resolve_metric")


def classify(metric: str, mean: float | None, threshold_ms: float,
             loss: float | None, anomaly: bool) -> str:
    """Map the indicators onto an SLA state.

    :param metric: the resolved metric.
    :param mean: the average of the samples, or None.
    :param threshold_ms: the threshold in use.
    :param loss: the loss percentage, or None if unknown.
    :param anomaly: the outcome of the anomaly check.
    :return: `nominal`, `degraded` or `critical`.

    Loss has a stricter rule than delay: any loss at all rules out `nominal`.
    """
    raise NotImplementedError("qos.classify")


def analyze(measurement_id: str, analysis: str | None, metric: str | None,
            values: list[float] | None, unit: str | None,
            packets_sent: int | None, packets_received: int | None,
            threshold_ms: float | None) -> AnalyticsResult:
    """Reduce a batch of samples to the QoS indicators and an SLA state.

    :param measurement_id: the measurement the samples belong to.
    :param analysis: the requested analysis, or None.
    :param metric: the metric of the measurement, or None.
    :param values: the samples. At least 1.
    :param unit: the unit the client sent, or None.
    :param packets_sent: packets sent, or None.
    :param packets_received: packets received, or None.
    :param threshold_ms: the threshold, or None for `DEFAULT_THRESHOLD_MS`.
    :return: the result, with the fields of the resolved metric filled in and
        the others left None.
    """
    raise NotImplementedError("qos.analyze")
