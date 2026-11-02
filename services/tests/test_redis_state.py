import fakeredis
import pytest

from aic_common.contract import JobStatus
from aic_common.redis_state import (
    create_job,
    get_job,
    set_job_status,
    set_job_fields,
    InvalidJobTransition,
    RedisStateError,
)


@pytest.fixture
def client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def job(client):
    create_job(client, "job-001", "m-001")
    return client


def test_valid_workflow_b_transitions(job):
    assert set_job_status(job, "job-001", JobStatus.STORED) is True
    assert set_job_status(job, "job-001", JobStatus.ANALYZED) is True
    assert set_job_status(job, "job-001", JobStatus.COMPLETED) is True

    assert get_job(job, "job-001")["status"] == "completed"


def test_valid_workflow_a_transition(client):
    create_job(client, "job-a", "m-001")
    assert set_job_status(client, "job-a", JobStatus.COMPLETED) is True
    assert get_job(client, "job-a")["status"] == "completed"


def test_valid_workflow_c_transitions(client):
    create_job(client, "job-c", "m-001")
    assert set_job_status(client, "job-c", JobStatus.ANALYZED) is True
    assert set_job_status(client, "job-c", JobStatus.COMPLETED) is True
    assert get_job(client, "job-c")["status"] == "completed"


def test_invalid_transition_is_rejected(job):
    with pytest.raises(InvalidJobTransition):
        set_job_status(job, "job-001", JobStatus.FAILED_REPORT_STORAGE)


def test_completed_job_cannot_regress(job):
    set_job_status(job, "job-001", JobStatus.STORED)
    set_job_status(job, "job-001", JobStatus.COMPLETED)

    with pytest.raises(InvalidJobTransition):
        set_job_status(job, "job-001", JobStatus.STORED)

    assert get_job(job, "job-001")["status"] == "completed"


def test_terminal_failure_cannot_be_overwritten(job):
    set_job_status(
        job, "job-001", JobStatus.FAILED_MEASUREMENT_STORAGE
    )

    with pytest.raises(InvalidJobTransition):
        set_job_status(job, "job-001", JobStatus.STORED)


def test_repeated_transition_is_idempotent(job):
    assert set_job_status(job, "job-001", JobStatus.STORED) is True
    assert set_job_status(job, "job-001", JobStatus.STORED) is False


def test_missing_job_raises_error(client):
    with pytest.raises(RedisStateError):
        set_job_status(client, "job-999", JobStatus.STORED)


def test_set_job_fields_cannot_bypass_status_guard(job):
    with pytest.raises(ValueError):
        set_job_fields(job, "job-001", {"status": "completed"})