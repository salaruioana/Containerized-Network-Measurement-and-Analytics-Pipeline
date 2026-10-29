# Contract

This contract is fixed. The Bruno collection, the public test suite and the grading tests depend on
it. The assignment page on the course website explains the reasons. Everything not in this document
is your design decision, for example object key layout and the internal structure of a service.

JSON is `snake_case` everywhere, in HTTP bodies and in queue messages.

## 1. Architecture

```
client -> api-gateway -> measurement.created -> storage-service
                      -> measurement.analysis.requested -> analytics-service
                         -> analysis.completed -> report-service
```

- Only the API gateway speaks HTTP. The other 3 services are queue consumers.
- Each service has one responsibility.
- Services are stateless. All state is in Redis or in the object store.
- Redis and the object store use persistent volumes.

## 2. Services

### 2.1 API Gateway (port 8080)

| Method and path | Success | Workflow |
|---|---|---|
| `POST /measurements` | 202 | A: store |
| `POST /measurements/analyze` | 202 | B: store and analyze |
| `POST /measurements/{measurement_id}/analyze` | 202 | C: analyze a stored measurement |
| `GET /jobs/{job_id}` | 200 | |
| `GET /measurements/{measurement_id}` | 200 | |
| `GET /reports/{report_id}` | 200 | |
| `GET /health` | 200 | |

There is no analytics endpoint. A client gets an analysis only through its job or its report.

**Submit body** (A and B):

```json
{
  "source_host": "h1",
  "target_host": "h2",
  "metric": "latency",
  "values_ms": [10, 11, 9, 45, 12],
  "unit": "ms",
  "timestamp": "2026-01-01T12:00:00Z",
  "packets_sent": 5,
  "packets_received": 5,
  "analysis": "latency_summary",
  "threshold_ms": 30
}
```

- Required: `source_host`, `target_host`, `metric`, and at least 1 sample. B also requires
  `analysis`.
- `values` is an alias for `values_ms`.
- If `timestamp` is missing, the gateway sets the time of arrival.

**Analyze-stored body** (C):

```json
{ "analysis": "latency_summary", "threshold_ms": 25 }
```

**Response to each `POST`**:

```json
{ "job_id": "job-…", "measurement_id": "m-…", "status": "queued" }
```

**`GET /jobs/{job_id}`**:

```json
{ "job_id": "job-…", "measurement_id": "m-…", "status": "completed", "report_id": "r-…", "result": { … } }
```

`report_id` and `result` appear when the report exists. Omit null fields.

**`GET /measurements/{measurement_id}`**:

```json
{
  "measurement_id": "m-…",
  "source_host": "h1",
  "target_host": "h2",
  "metric": "latency",
  "timestamp": "2026-01-01T12:00:00Z",
  "object_path": "measurements/h1-h2/m-….json",
  "status": "stored"
}
```

**`GET /reports/{report_id}`**:

```json
{ "report_id": "r-…", "job_id": "job-…", "measurement_id": "m-…", "result": { … } }
```

With `?format=csv`, the gateway returns the same report as CSV (`Content-Type: text/csv`): 1 header
row and 1 data row, 1 column per field in `result`.

**Errors** (body `{"error": "…"}`):

| Status | When |
|---|---|
| 400 | Invalid request body |
| 404 | Unknown measurement, job or report |
| 503 | Publish to RabbitMQ failed |

**Rules**:

- Each `POST` returns directly after the message is on the queue. It does not wait for the result.
- The gateway saves the job as `queued` before it publishes. If the publish fails, it deletes the
  job.
- A rejected request creates no job. For C, an unknown measurement returns 404 before a job exists.
- The gateway never writes a measurement, an analysis or a report.

### 2.2 Storage Service

- Consumes `measurement.created`.
- Writes the raw measurement to the `measurements` bucket.
- Writes the measurement metadata to Redis. It is the only writer of this metadata.
- Sets the job to `completed` (A), or to `stored` and publishes to `measurement.analysis.requested`
  (B).

### 2.3 QoS Analytics Service

- Consumes `measurement.analysis.requested`.
- Reads the raw measurement from the object store.
- Sets the job to `analyzed` and publishes the result to `analysis.completed`.
- Persists nothing.

**Result**:

```json
{
  "measurement_id": "m-001",
  "metric": "latency",
  "unit": "ms",
  "sample_count": 5,
  "average_latency_ms": 17.4,
  "min_latency_ms": 9,
  "max_latency_ms": 45,
  "jitter_ms": 18.0,
  "threshold_ms": 30,
  "anomaly_detected": true,
  "quality": "critical"
}
```

The field names are in `aic_common.contract.AnalyticsResult`. `measurement_id`, `metric` and
`sample_count` are required. Implement at least 2 analytics
functions. Omit the fields of the functions you do not implement. The fields you do implement must
be correct.

For samples `v1 … vn`:

| Field | Rule |
|---|---|
| `average_latency_ms` | Mean, rounded to 1 decimal |
| `min_latency_ms`, `max_latency_ms` | Min and max, rounded to whole ms |
| `jitter_ms` | Mean of `abs(v[i] - v[i-1])`, 0 for 1 sample. If `metric` is `jitter`, the mean of the samples |
| `packet_loss_percent` | `(sent - min(received, sent)) / sent * 100`. Only if both counts are present |
| `threshold_ms` | The threshold used. Default 30 |
| `anomaly_detected` | `max(samples) > threshold_ms`, or packet loss > 5% |
| `quality` | `critical` if anomaly. Else `nominal` if average <= 0.6 × `threshold_ms` and no packet loss. Else `degraded` |

Rounding is half away from zero. Python `round()` does not do this.

### 2.4 Report Service

- Consumes `analysis.completed`.
- Derives the report id from the measurement id: `m-a1b2` gives `r-a1b2`.
- Writes the report document, with the result embedded, to the `reports` bucket.
- Writes `report_id` to the job and sets the job to `completed`.

One measurement has exactly one report. A new analysis overwrites it. The `job_id` in the report
is the job that wrote it last.

## 3. RabbitMQ

| Queue | Consumed by | Published by |
|---|---|---|
| `measurement.created` | storage service | gateway |
| `measurement.analysis.requested` | analytics service | storage service, gateway |
| `analysis.completed` | report service | analytics service |
| `measurement.failed` | nobody | any service, optional |

- The first 3 queues are required. Their names are in `aic_common.contract.Queues`.
- Each consumer subscribes to 1 queue and publishes to at most 1 queue. Error queues are an
  exception.
- Delivery is at-least-once. A consumer that runs twice on the same message must not create a
  second artifact.
- The message format is your design. It is internal to your services.

## 4. Object Storage

| Bucket | Content |
|---|---|
| `measurements` | Raw measurements. The JSON format is your decision |
| `reports` | Report documents, with the analysis embedded |
| `errors` | Optional |

The `reports` bucket contains no CSV objects. The gateway renders CSV on read.

## 5. Redis

```
job:job-001
  measurement_id  m-001
  status          completed
  report_id       r-001
```

- A job is a hash. Each service updates only its own fields. Do not read, change and write back
  the whole record.
- The job holds a reference to the report, never a copy of the analysis.
- Measurement metadata is also in Redis, for example as `meas:<measurement_id>`.
- Redis runs with append-only persistence on a volume.

## 6. Job Lifecycle

| Status | Written by | Meaning |
|---|---|---|
| `queued` | gateway | Message is on a queue |
| `stored` | storage | Measurement written, analysis follows |
| `analyzed` | analytics | Result sent to the report service |
| `completed` | storage (A), report (B, C) | Last stage finished |
| `failed_measurement_storage` | storage | Measurement write failed |
| `failed_analysis` | analytics | Analysis failed |
| `failed_report_storage` | report | Report write failed |

| Workflow | Path |
|---|---|
| A | `queued → completed` |
| B | `queued → stored → analyzed → completed` |
| C | `queued → analyzed → completed` |

Ids: measurements `m-`, jobs `job-`, reports `r-`. The prefixes are in
`aic_common.contract`.

## 7. Deployment

- `docker compose up --build` starts the full system: 4 services, RabbitMQ, Garage and Redis.
- Each service has its own image and runs 1 process.
- Redis and Garage use named volumes. `docker compose down` and `up` keeps all jobs, measurements
  and reports.
- Kubernetes: submit manifests or a Helm chart, not both. At least 1 stateless service runs with
  more than 1 replica.
- All credentials, hosts and ports come from the environment.
