# Evidence checklist

Every file starts with the command that produced it, e.g.
`{ echo '$ docker compose ps'; docker compose ps; } > evidence/compose/ps.txt`.
Screenshots go next to the text files as `.png`.

## API behavior and workflows (Compose)

| Done | Evidence | How | File | Owner |
|---|---|---|---|---|
| [ ] | Workflow A: submit, poll job, get measurement | `curl` / Bruno | `api/workflow-a.txt` | Yaron, Sebastian |
| [ ] | Workflow B: `queued → stored → analyzed → completed` | `curl`, poll `GET /jobs/{id}` | `api/workflow-b.txt` | Attila, Ioana |
| [ ] | Workflow C on a stored measurement | `curl` | `api/workflow-c.txt` | Attila, Ioana |
| [ ] | Report as JSON and as CSV | `curl /reports/{id}` and `?format=csv` | `api/report.txt` | Yaron |
| [ ] | 400, 404, 503 (stop RabbitMQ, POST, no `job:*` left) | `curl -i` | `api/errors.txt` | Yaron |
| [ ] | Request from the ping script | `make simulate` | `api/simulator.txt` | Oliver |
| [ ] | Duplicate delivery: same `report_id`, one object | Workflow C twice + `make buckets` | `api/duplicates.txt` | Ioana |

## Infrastructure (Compose)

| Done | Evidence | How | File | Owner |
|---|---|---|---|---|
| [ ] | Stack starts with one command | `docker compose up --build`, `docker compose ps` | `compose/up.txt`, `compose/ps.txt` | Oliver |
| [ ] | Messages pass through RabbitMQ | management UI (`make up-dev`, `localhost:15672`), consumer logs | `rabbitmq/*.png`, `rabbitmq/logs.txt` | Ioana |
| [ ] | Raw measurement and report in Garage, no CSV in `reports` | `make buckets`, downloaded objects | `storage/buckets.txt` | Sebastian, Ioana |
| [ ] | Job and measurement state in Redis, no analysis copy | `redis-cli KEYS "job:*"`, `HGETALL job:<id>`, `HGETALL meas:<id>` | `redis/state.txt` | Oliver |
| [ ] | Durability: job survives a Redis restart and `down`/`up` | see below | `redis/durability.txt` | Oliver |

Durability:

```bash
curl -s -XPOST localhost:8080/measurements -H 'Content-Type: application/json' \
  -d '{"source_host":"h1","target_host":"h2","metric":"latency","values_ms":[10,11,12]}'
docker compose restart redis && curl -s localhost:8080/jobs/<job_id>
docker compose down && docker compose up -d && curl -s localhost:8080/jobs/<job_id>
curl -s localhost:8080/measurements/<measurement_id>
```

## Kubernetes

| Done | Evidence | How | File | Owner |
|---|---|---|---|---|
| [ ] | Deploy | `kubectl apply -f k8s/manifests/` | `k8s/apply.txt` | Oliver |
| [ ] | Pods, deployments, services, PVCs (2 replicas visible) | `kubectl get pods,deployments,services,pvc,jobs -n aic` | `k8s/get.txt` | Oliver |
| [ ] | Gateway reachable | `kubectl port-forward ...` + `curl /health` | `k8s/health.txt` | Oliver |
| [ ] | End-to-end Workflow B on the cluster | `curl` via port-forward | `k8s/workflow-b.txt` | Oliver |
| [ ] | Work spread over replicas | `kubectl logs -n aic -l app=analytics-service --prefix` | `k8s/replica-logs.txt` | Oliver |
| [ ] | Ping script against the cluster | `make simulate API=http://127.0.0.1:18080` | `k8s/simulator.txt` | Oliver |
