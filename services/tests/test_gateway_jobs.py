from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pika
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
import pytest

from api_gateway.config import Settings
from api_gateway.main import (
    get_job_store,
    get_publisher,
    job_store_error_handler,
    publish_error_handler,
)
from api_gateway.publisher import PublishError, Publisher
from api_gateway.service import create_job_and_publish
from api_gateway.store import JobStoreError


class FakeJobStore:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, str]] = {}
        self.fail_create = False
        self.fail_delete = False

    def create_job(self, job_id: str, measurement_id: str) -> None:
        if self.fail_create:
            raise JobStoreError("create failed")
        self.jobs[f"job:{job_id}"] = {
            "measurement_id": measurement_id,
            "status": "queued",
        }

    def delete_job(self, job_id: str) -> None:
        if self.fail_delete:
            raise RuntimeError("delete failed")
        self.jobs.pop(f"job:{job_id}", None)


class FakePublisher:
    def __init__(self, store: FakeJobStore) -> None:
        self.store = store
        self.published: list[tuple[str, dict]] = []
        self.fail = False

    def publish(self, queue: str, payload: dict) -> None:
        job_id = payload["job_id"]
        assert f"job:{job_id}" in self.store.jobs
        if self.fail:
            raise PublishError("publish failed")
        self.published.append((queue, payload))


def build_message(job_id: str, measurement_id: str) -> dict:
    return {"job_id": job_id, "measurement_id": measurement_id}


def test_create_job_and_publish_success() -> None:
    store = FakeJobStore()
    publisher = FakePublisher(store)

    response = create_job_and_publish(
        store, publisher, "measurement.created", build_message
    )

    assert response.job_id.startswith("job-")
    assert response.measurement_id.startswith("m-")
    assert response.status == "queued"
    assert store.jobs[f"job:{response.job_id}"] == {
        "measurement_id": response.measurement_id,
        "status": "queued",
    }
    assert publisher.published == [
        (
            "measurement.created",
            {
                "job_id": response.job_id,
                "measurement_id": response.measurement_id,
            },
        )
    ]


def test_create_job_and_publish_uses_workflow_c_measurement_id() -> None:
    store = FakeJobStore()
    publisher = FakePublisher(store)

    response = create_job_and_publish(
        store,
        publisher,
        "measurement.analysis.requested",
        build_message,
        measurement_id="m-existing",
    )

    assert response.measurement_id == "m-existing"
    assert publisher.published[0][1]["measurement_id"] == "m-existing"


def test_publish_error_rolls_back_without_sending_message() -> None:
    store = FakeJobStore()
    publisher = FakePublisher(store)
    publisher.fail = True

    with pytest.raises(PublishError):
        create_job_and_publish(store, publisher, "measurement.created", build_message)

    assert store.jobs == {}
    assert publisher.published == []


def test_rollback_error_does_not_replace_publish_error() -> None:
    store = FakeJobStore()
    store.fail_delete = True
    publisher = FakePublisher(store)
    publisher.fail = True

    with pytest.raises(PublishError, match="publish failed"):
        create_job_and_publish(store, publisher, "measurement.created", build_message)


def test_job_store_error_prevents_publish() -> None:
    store = FakeJobStore()
    store.fail_create = True
    publisher = Mock()

    with pytest.raises(JobStoreError):
        create_job_and_publish(store, publisher, "measurement.created", build_message)

    publisher.publish.assert_not_called()


def make_http_test_app(store: FakeJobStore, publisher: FakePublisher) -> TestClient:
    test_app = FastAPI()
    test_app.add_exception_handler(PublishError, publish_error_handler)
    test_app.add_exception_handler(JobStoreError, job_store_error_handler)

    @test_app.post("/test/jobs", status_code=202)
    def create_test_job(
        job_store=Depends(get_job_store),
        message_publisher=Depends(get_publisher),
    ):
        return create_job_and_publish(
            job_store,
            message_publisher,
            "measurement.created",
            build_message,
        )

    test_app.dependency_overrides[get_job_store] = lambda: store
    test_app.dependency_overrides[get_publisher] = lambda: publisher
    return TestClient(test_app)


def test_http_success_and_publish_failure() -> None:
    store = FakeJobStore()
    publisher = FakePublisher(store)
    client = make_http_test_app(store, publisher)

    success = client.post("/test/jobs")
    assert success.status_code == 202
    assert set(success.json()) == {"job_id", "measurement_id", "status"}

    publisher.fail = True
    failure = client.post("/test/jobs")
    assert failure.status_code == 503
    assert failure.json() == {"error": "message broker unavailable"}
    assert len(store.jobs) == 1


def test_http_store_failure() -> None:
    store = FakeJobStore()
    store.fail_create = True
    publisher = FakePublisher(store)
    client = make_http_test_app(store, publisher)

    response = client.post("/test/jobs")

    assert response.status_code == 503
    assert response.json() == {"error": "job store unavailable"}
    assert publisher.published == []


class FakeChannel:
    def __init__(self) -> None:
        self.declarations: list[dict] = []
        self.confirm_calls = 0
        self.published: list[dict] = []
        self.publish_error: Exception | None = None

    def confirm_delivery(self) -> None:
        self.confirm_calls += 1

    def queue_declare(self, **kwargs) -> None:
        self.declarations.append(kwargs)

    def basic_publish(self, **kwargs):
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append(kwargs)
        return True


class FakeConnection:
    def __init__(self, channel: FakeChannel) -> None:
        self.channel_instance = channel
        self.closed = False

    def channel(self) -> FakeChannel:
        return self.channel_instance

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def settings() -> Settings:
    return Settings("rabbitmq", 5672, "guest", "guest", "redis", 6379, "INFO")


def test_publisher_declares_once_confirms_and_persists(settings, monkeypatch) -> None:
    channel = FakeChannel()
    connection = FakeConnection(channel)
    monkeypatch.setattr(pika, "BlockingConnection", lambda parameters: connection)

    publisher = Publisher(settings)
    publisher.publish("measurement.created", {"message": "one"})
    publisher.publish("measurement.created", {"message": "two"})

    assert channel.confirm_calls == 1
    assert channel.declarations == [{"queue": "measurement.created", "durable": True}]
    published = channel.published[0]
    assert published["routing_key"] == "measurement.created"
    assert published["mandatory"] is True
    assert json.loads(published["body"]) == {"message": "one"}
    assert published["properties"].content_type == "application/json"
    assert published["properties"].delivery_mode == 2


def test_publisher_converts_nack_and_unroutable_errors(settings, monkeypatch) -> None:
    for error_type in (pika.exceptions.NackError, pika.exceptions.UnroutableError):
        channel = FakeChannel()
        channel.publish_error = error_type([])
        connection = FakeConnection(channel)
        monkeypatch.setattr(pika, "BlockingConnection", lambda parameters: connection)
        publisher = Publisher(settings)

        with pytest.raises(PublishError):
            publisher.publish("measurement.created", {"message": "one"})


def test_publisher_reconnects_once_after_connection_error(settings, monkeypatch) -> None:
    first_channel = FakeChannel()
    first_channel.publish_error = pika.exceptions.AMQPConnectionError()
    second_channel = FakeChannel()
    connections = [FakeConnection(first_channel), FakeConnection(second_channel)]
    monkeypatch.setattr(
        pika,
        "BlockingConnection",
        lambda parameters: connections.pop(0),
    )

    publisher = Publisher(settings)
    publisher.publish("measurement.created", {"message": "one"})
    publisher.publish("measurement.created", {"message": "two"})

    assert first_channel.published == []
    assert second_channel.published[0]["routing_key"] == "measurement.created"
    assert first_channel.confirm_calls == 1
    assert second_channel.confirm_calls == 1
