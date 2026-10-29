"""The public API suite. A subset; grading runs more against the same contract.

`TestHealth` passes today. The rest will not until you have built Workflow A.
"""

import requests


class TestHealth:
    def test_the_gateway_answers(self, api):
        response = requests.get(f"{api}/health", timeout=10)
        assert response.status_code == 200
        assert response.json()["status"]


class TestWorkflowA:
    def test_a_submitted_measurement_is_accepted_immediately(self, api, measurement):
        response = requests.post(f"{api}/measurements", json=measurement, timeout=10)

        assert response.status_code == 202
        body = response.json()
        # The answer comes back before the work does, so `queued` is correct
        # here and `completed` would mean the endpoint blocked.
        assert body["status"] == "queued"
        assert body["measurement_id"].startswith("m-")
        assert body["job_id"].startswith("job-")

    def test_the_job_reaches_completed(self, api, measurement, await_job):
        ack = requests.post(f"{api}/measurements", json=measurement, timeout=10).json()

        job = await_job(ack["job_id"])

        assert job["status"] == "completed"
        assert job["measurement_id"] == ack["measurement_id"]
        # Storage was the whole job, so there is nothing to report on.
        assert "report_id" not in job

    def test_the_stored_measurement_is_retrievable(self, api, measurement, await_job):
        ack = requests.post(f"{api}/measurements", json=measurement, timeout=10).json()
        await_job(ack["job_id"])

        body = requests.get(f"{api}/measurements/{ack['measurement_id']}", timeout=10).json()

        assert body["source_host"] == "h1"
        assert body["target_host"] == "h2"
        assert body["metric"] == "latency"
        # Metadata points at the raw object rather than embedding it.
        assert ack["measurement_id"] in body["object_path"]

    def test_an_invalid_body_is_rejected_without_creating_a_job(self, api):
        response = requests.post(f"{api}/measurements", json={}, timeout=10)

        assert response.status_code == 400
        assert "job_id" not in response.json()

    def test_an_unknown_job_is_a_404(self, api):
        assert requests.get(f"{api}/jobs/job-does-not-exist", timeout=10).status_code == 404
