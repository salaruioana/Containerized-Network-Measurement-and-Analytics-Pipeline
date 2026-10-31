# Containerized Network Measurement and Analytics Pipeline

Four Python services (API gateway, storage, QoS analytics, report) connected by RabbitMQ, with
job state in Redis and raw measurements and reports in Garage (S3). The fixed interface is in
[`CONTRACT.md`](CONTRACT.md), the architecture in [`documentation`](documentation/ARCHITECTURE.md).

## Run with Docker Compose

```bash
cp .env.example .env
docker compose up --build          # or: make up (detached)
curl -s localhost:8080/health
```

| Container | Role | Port |
|---|---|---|
| `api-gateway` | the only HTTP service | `8080` |
| `storage-service`, `analytics-service`, `report-service` | queue consumers | none |
| `rabbitmq` | message broker | internal |
| `redis` | job state and measurement metadata, append-only | internal |
| `garage` | S3 object store, single-node mode, imports the S3 key from `.env` | internal |
| `garage-init` | one-shot: creates the buckets `measurements`, `reports`, `errors` | none |

- Redis, Garage and RabbitMQ use named volumes. `docker compose down` keeps all data;
  `docker compose down -v` (`make reset`) deletes it.
- Credentials are applied when a volume is first created. After changing them in `.env`, run
  `docker compose down -v`.
- Scale a consumer: `docker compose up -d --scale analytics-service=2`.

**Development overlay.** `make up-dev` (`docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d`)
also publishes RabbitMQ (`5672`, management UI on `http://localhost:15672`), Redis (`6379`)
and Garage S3 (`3900`) on localhost, so a service can run on the host while you work on it:

```bash
docker compose stop storage-service
cd services && set -a && . ../.env && set +a
RABBITMQ_HOST=localhost REDIS_HOST=localhost S3_ENDPOINT=http://localhost:3900 python -m storage_service.main
```

## Environment

Every service reads its configuration from these variables. Compose takes them from `.env`;
Kubernetes from `k8s/manifests/05-config.yaml` and `06-secret.yaml`.

| Variable | Compose value |
|---|---|
| `RABBITMQ_HOST`, `RABBITMQ_PORT` | `rabbitmq`, `5672` |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | from `.env` |
| `REDIS_HOST`, `REDIS_PORT` | `redis`, `6379` |
| `S3_ENDPOINT`, `S3_REGION` | `http://garage:3900`, `garage` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | from `.env` |
| `LOG_LEVEL` | `INFO` |

S3 client: `boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=S3_REGION, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)`.
The buckets exist before the services start; services never create buckets. RabbitMQ starts
empty; the services declare the queues they use.

## Tests

```bash
cd services && pytest                                   # unit tests (make test)
cd tests && API_BASE_URL=http://localhost:8080 pytest   # public API tests (make test-api)
python3 scripts/measurement_simulator.py                # ping simulator (make simulate)
```

Manual requests: open `bruno/` as a Bruno collection.

## Run on Kubernetes (Minikube)

```bash
minikube start
cp .env.example .env
make k8s-images                            # docker compose build + minikube image load
kubectl apply -f k8s/manifests/
kubectl get pods,deployments,services,pvc,jobs -n aic
kubectl port-forward -n aic svc/api-gateway 18080:8080
curl -s http://127.0.0.1:18080/health
```

- Namespace `aic`. The gateway is also on NodePort `30080` (`http://$(minikube ip):30080`).
- `api-gateway` and `analytics-service` run 2 replicas; storage and report run 1.
- Redis, Garage and RabbitMQ use PVCs with the `Recreate` strategy, so a rollout never mounts a
  `ReadWriteOnce` volume twice. The `garage-init` Job creates the buckets.
- After rebuilding images: `make k8s-images k8s-restart`. Remove everything, data included:
  `kubectl delete namespace aic`.

## Windows (PowerShell)

The Makefile works with GNU Make on Windows. Commands in this README are written for bash; in
PowerShell use `curl.exe` instead of `curl`, `Select-String` instead of `grep`, and a pipe instead
of `<`, e.g. `Get-Content scripts/list_buckets.py | kubectl exec -i -n aic deploy/api-gateway -- python -`.

## Evidence

The checklist for the report is in [`evidence/README.md`](evidence/README.md).
