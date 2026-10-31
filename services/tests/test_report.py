
import pytest

from report_service.main import report_id_for, build_report


def test_report_id_for():
    assert report_id_for("m-a1b2") == "r-a1b2"


def test_report_id_for_invalid_prefix():
    with pytest.raises(ValueError):
        report_id_for("x-a1b2")


def test_report_id_for_empty_suffix():
    with pytest.raises(ValueError):
        report_id_for("m-")


def test_build_report():
    result = {
        "measurement_id": "m-a1b2",
        "metric": "latency",
        "sample_count": 2,
        "average_latency_ms": 15.0,
    }

    report = build_report("job-123", "m-a1b2", result)

    assert report == {
        "report_id": "r-a1b2",
        "job_id": "job-123",
        "measurement_id": "m-a1b2",
        "result": result,
    }
    assert report["result"] is result
