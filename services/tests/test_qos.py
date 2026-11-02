"""Your specification for the analytics. Red until you write them.

Every expected number here is taken from the assignment's worked examples. It
covers a subset: §1.3.3 asks for at least two analytics functions, so a class
whose function you did not implement can be deleted. The ones you keep have to
be right.

    cd services && pytest
"""

import pytest

from analytics_service import qos


def analyze(values, **kwargs):
    kwargs.setdefault("analysis", "latency_summary")
    kwargs.setdefault("metric", "latency")
    kwargs.setdefault("unit", None)
    kwargs.setdefault("packets_sent", None)
    kwargs.setdefault("packets_received", None)
    kwargs.setdefault("threshold_ms", None)
    return qos.analyze("m-test", values=values, **kwargs)


class TestRounding:
    def test_rounds_half_away_from_zero(self):
        assert qos.round_half_up(17.45) == 17.5
        assert qos.round_half_up(17.44) == 17.4

    def test_rounds_to_whole_numbers(self):
        assert qos.round_half_up(8.5, 0) == 9


class TestSummary:
    def test_matches_the_assignment_example(self):
        result = analyze([10, 11, 9, 45, 12], threshold_ms=30)
        assert result.average_latency_ms == 17.4
        assert result.min_latency_ms == 9
        assert result.max_latency_ms == 45
        assert result.threshold_ms == 30
        assert result.anomaly_detected is True
        assert result.quality == "critical"

    def test_matches_the_workflow_c_example(self):
        result = analyze([10, 11, 13, 12, 15], threshold_ms=25)
        assert result.average_latency_ms == 12.2
        assert result.min_latency_ms == 10
        assert result.max_latency_ms == 15
        assert result.anomaly_detected is False
        assert result.quality == "nominal"

    def test_counts_the_samples(self):
        assert analyze([1, 2, 3]).sample_count == 3

    def test_latency_bounds_are_whole_milliseconds(self):
        result = analyze([9.4, 12.6])
        assert result.min_latency_ms == 9
        assert result.max_latency_ms == 13


class TestJitter:
    def test_is_the_mean_successive_difference(self):
        assert qos.jitter([10, 30, 10, 30, 10]) == 20.0

    def test_distinguishes_a_drift_from_an_oscillation(self):
        # Same mean and spread as above, but only one of four gaps jumps.
        assert qos.jitter([10, 10, 10, 30, 30]) == 5.0

    def test_a_single_sample_has_no_pair_to_compare(self):
        assert qos.jitter([10]) == 0.0


class TestPacketLoss:
    def test_percentage(self):
        assert qos.packet_loss_percent(10, 9) == 10.0

    def test_missing_counts_are_not_zero_loss(self):
        assert qos.packet_loss_percent(10, None) is None
        assert qos.packet_loss_percent(None, 10) is None

    def test_more_received_than_sent_is_not_negative_loss(self):
        assert qos.packet_loss_percent(10, 12) == 0.0

    def test_loss_over_five_percent_is_an_anomaly(self):
        result = analyze([10, 11], threshold_ms=30, packets_sent=100, packets_received=90)
        assert result.packet_loss_percent == 10.0
        assert result.anomaly_detected is True
        assert result.quality == "critical"


class TestAnomalyDetection:
    def test_the_worst_sample_decides_not_the_average(self):
        # Average 68 ms is comfortable; the one 300 ms spike is not.
        result = analyze([10, 10, 10, 10, 300], threshold_ms=100)
        assert result.average_latency_ms == 68.0
        assert result.anomaly_detected is True

    def test_a_sample_exactly_on_the_ceiling_does_not_breach_it(self):
        assert analyze([30, 10], threshold_ms=30).anomaly_detected is False

    def test_defaults_to_thirty_milliseconds(self):
        result = analyze([10, 11, 35])
        assert result.threshold_ms == 30
        assert result.anomaly_detected is True


class TestClassification:
    def test_nominal_needs_headroom_to_the_ceiling(self):
        # 18 ms is exactly 60% of 30 ms: still comfortable.
        assert analyze([18, 18], threshold_ms=30).quality == "nominal"

    def test_eroding_toward_the_ceiling_is_degraded(self):
        result = analyze([25, 25], threshold_ms=30)
        assert result.anomaly_detected is False
        assert result.quality == "degraded"

    def test_any_loss_at_all_disqualifies_nominal(self):
        result = analyze([10, 10], threshold_ms=30, packets_sent=100, packets_received=99)
        assert result.packet_loss_percent == 1.0
        assert result.anomaly_detected is False
        assert result.quality == "degraded"


class TestMetricMapping:
    """Delete this class if you only implemented latency analytics."""

    def test_throughput_gets_throughput_fields(self):
        result = analyze([90, 100, 95], analysis="throughput_summary", metric="throughput")
        assert result.throughput_avg_mbps == 95.0
        assert result.throughput_min_mbps == 90.0
        assert result.throughput_max_mbps == 100.0
        assert result.anomaly_detected is False

    def test_latency_fields_are_absent_from_a_throughput_result(self):
        result = analyze([90], analysis="throughput_summary", metric="throughput")
        assert "average_latency_ms" not in result.model_dump(exclude_none=True)

    def test_jitter_samples_are_averaged_rather_than_differenced(self):
        # The client submitted jitter readings, not delays to diff.
        result = analyze([4, 6, 5], analysis="jitter_summary", metric="jitter")
        assert result.jitter_ms == 5.0

    @pytest.mark.parametrize("unit", ["ms", "s"])
    def test_a_supplied_unit_wins_over_the_default(self, unit):
        assert analyze([10], unit=unit).unit == unit

# --- Our edge cases See Task(#29), beyond the supplied tests above ---

def dumped(result):
    """The result as it goes on the queue: None fields dropped."""
    return result.model_dump(exclude_none=True)


class TestSingleSample:
    def test_one_sample_has_zero_jitter_and_equal_bounds(self):
        result = analyze([12])
        assert result.sample_count == 1
        assert result.jitter_ms == 0.0
        assert result.min_latency_ms == result.max_latency_ms == 12
        assert result.average_latency_ms == 12.0


class TestMissingPacketCounts:
    @pytest.mark.parametrize("sent, received", [(10, None), (None, 10), (None, None)])
    def test_no_loss_field_without_both_counts(self, sent, received):
        result = analyze([10, 11], packets_sent=sent, packets_received=received)
        assert "packet_loss_percent" not in dumped(result)

    def test_unknown_loss_does_not_block_nominal(self):
        assert analyze([10, 10], threshold_ms=30).quality == "nominal"


class TestMoreReceivedThanSent:
    def test_loss_is_zero_and_still_nominal(self):
        result = analyze([10, 10], packets_sent=10, packets_received=12)
        assert result.packet_loss_percent == 0.0
        assert result.anomaly_detected is False
        assert result.quality == "nominal"


class TestThresholdBoundaries:
    def test_max_equal_to_threshold_is_no_anomaly(self):
        result = analyze([5, 25], threshold_ms=25)
        assert result.anomaly_detected is False

    def test_just_above_threshold_is_an_anomaly(self):
        assert analyze([5, 25.1], threshold_ms=25).anomaly_detected is True

    @pytest.mark.parametrize("threshold, average", [(50, 30), (3, 1.8)])
    def test_average_exactly_sixty_percent_is_nominal(self, threshold, average):
        result = analyze([average, average], threshold_ms=threshold)
        assert result.average_latency_ms == average
        assert result.quality == "nominal"


class TestLossBoundary:
    def test_exactly_five_percent_is_no_anomaly_but_degraded(self):
        result = analyze([10, 10], threshold_ms=30, packets_sent=100, packets_received=95)
        assert result.packet_loss_percent == 5.0
        assert result.anomaly_detected is False
        assert result.quality == "degraded"


class TestDefaultThreshold:
    def test_missing_threshold_falls_back_to_thirty(self):
        result = analyze([10, 30])
        assert result.threshold_ms == 30
        assert result.anomaly_detected is False

    def test_default_threshold_still_catches_a_spike(self):
        assert analyze([10, 31]).anomaly_detected is True


class TestInvalidInput:
    def test_no_samples_raises(self):
        with pytest.raises(ValueError):
            analyze([])

    def test_unknown_metric_raises(self):
        with pytest.raises(ValueError):
            analyze([10], analysis="foo_summary")