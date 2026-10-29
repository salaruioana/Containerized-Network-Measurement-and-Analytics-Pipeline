# Assignment 2: Starting Template

The full assignment is on the course website. Read [`CONTRACT.md`](CONTRACT.md) first. You cannot
change it.

## Getting Started

We recommend [uv](https://docs.astral.sh/uv/pip/environments/). It is faster than pip and common
in industry. Plain `pip` and any other virtual environment also work.

```bash
cp .env.example .env
uv venv && source .venv/bin/activate
uv pip install -r services/requirements-dev.txt

cd services
pytest
```

`pytest` is red on purpose. `services/tests/test_qos.py` is the specification for the analytics.
Make it green, and delete the test classes for the functions you do not implement.

`services/aic_common/contract.py` holds the names the contract fixes: queues, buckets, job statuses,
id prefixes and the result fields. Everything else in `services/` is yours to write.

### Run the Gateway

The gateway starts without a broker, an object store or Redis:

```bash
docker build --target api-gateway -t aic-template/api-gateway:dev ./services
docker run --rm -p 8080:8080 aic-template/api-gateway:dev
curl -s localhost:8080/health        # {"status":"UP","service":"api-gateway"}
```

### Send Traffic

The simulator sends pings between simulated hosts to the gateway. Configure the hosts, the rounds
and the latency range in `scripts/simulator.yaml`.

```bash
uv pip install -r scripts/requirements.txt
python3 scripts/measurement_simulator.py
python3 scripts/measurement_simulator.py --api http://$(minikube ip):30080
```

### Run the Public API Tests

```bash
cd tests
uv pip install -r requirements.txt
API_BASE_URL=http://localhost:8080 pytest
```

This suite is a subset. Grading runs more tests against the same contract.

## Kubernetes

`k8s/` has empty skeletons for plain manifests and for a Helm chart. Submit `manifests/` or `helm/`,
but not both.
