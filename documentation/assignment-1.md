# Containerized Network Measurement and Analytics Pipeline

## Exercise Description

### Context

In this assignment, you will deploy a small distributed backend analysis and report application using containers. This assignment focuses on collecting and analyzing **network measurement data**. The template contains a ping script that simulates pings between hosts and submits the measurements.

The goal is to build a containerized backend pipeline that receives network measurement results from the ping script, stores raw measurements, analyzes them asynchronously, and generates reports.

The system must first be deployable with **Docker Compose** and then with **Kubernetes**.

### Learning Objectives

After completing this assignment, students should be able to:

- Design a small distributed application using multiple containerized services.

- Use **[Docker](https://docs.docker.com/engine/)** to containerize individual services.

- Use **[Docker Compose](https://docs.docker.com/compose/)** to deploy and test a multi-service application locally.

- Use **[Kubernetes](https://kubernetes.io/docs/home/)** to deploy the same application as replicated services.

- Use **[RabbitMQ](https://www.rabbitmq.com/docs)** for asynchronous communication between services.

- Use an **S3-compatible object store** for raw measurements and for the reports that embed their analysis results. For this assignment we recommend using **[Garage](https://garagehq.deuxfleurs.fr/documentation/quick-start/)**.

- Use **[Redis](https://redis.io/docs/latest/)** to hold shared job and measurement state, such that services remain stateless and can be replicated.

- Understand the difference between synchronous API calls and asynchronous message-driven processing.

- Understand how service decomposition prepares an application for later serverless deployment.

(starting-template)=
### Starting Template

Start from the template. It contains the service structure, the wire contract in `CONTRACT.md`, the
ping script that generates the measurements, a Bruno collection for the API, and a unit test suite
for the analytics that is red until you implement them. The `docker-compose.yml` and the files under
`k8s/` are empty, and every service is a skeleton: that part is the assignment. **All services must be implemented in Python.**

```{raw} html
<a class="download-card" href="_static/measurement-pipeline-template.zip" download>
  <span>
    <span class="download-card-title">Starting template (ZIP, 40&nbsp;KB)</span>
    <span class="download-card-meta">Service skeleton, ping script, Bruno collection, empty Compose and Kubernetes files</span>
  </span>
</a>
```

Read `CONTRACT.md` first. The endpoints, the queue names, the job statuses and the analytics results
in it are fixed, because the provided tests and the grading tests check them. Everything else is
your design.

## System Overview

Students must implement a small distributed network measurement and analytics system.

The ping script from the template acts as the network client. It simulates pings between hosts and submits measurement data such as latency, packet loss, throughput, or jitter to the analytics and report application.

The backend application receives data through the API Gateway, processes it asynchronously and exposes the results through the same public API. An analysis is computed on demand and handed to the Report Service on the queue.

The system separates two kinds of state: **Durable artifacts** include raw measurements and reports. Raw measurement artifacts are immutable once stored. Reports may be updated or replaced when the same measurement is reanalyzed. Submitting the same measurement and/or requesting the same report is idempotent; **Job and measurement state** is mutable and is written concurrently by several services.

**Architectural constraints.** These apply to the whole system and are part of the grading. A solution that works but violates them has not met the assignment.

- **Single service, single responsibility.** Each service does one thing only. A service that both analyses and reports has merged two phases and is not a decomposed system.

- **Stateless services.** No service may keep request or job state in process memory. Restarting a container, or running a second replica of it, must not change what the system does. Multiple replicas appear as a single system to the client.

- **State placement is fixed.** Job state, measurement metadata, raw measurements, analyses and reports must be saved in a suitable format and must be stored in the proper service.

- **Durable state.** Raw measurements, reports, and job and measurement states must be stored in persistent volumes. Restarting the stack must not lose submitted jobs or stored measurements.

The system must follow the general architecture:

::::{container} centered
```{mermaid}
flowchart TD
    client["Ping Script"]
    gw["Measurement<br/>API Gateway"]
    redis[("Redis")]
    q1(["measurement.created"])
    storage["Measurement<br/>Storage Service"]
    q2(["measurement.analysis.requested"])
    analytics["QoS Analytics<br/>Service"]
    q3(["analysis.completed"])
    report["Report Service"]
    objstore[("Object Storage")]

    client --> gw
    gw -.-> redis
    gw --> q1
    storage --> q2
    gw --> q2
    q1 --> storage
    storage --> objstore
    q2 --> analytics
    analytics --> q3
    q3 --> report
    report --> objstore
```
::::

Only the API Gateway speaks HTTP. Every other service is a queue consumer, and all shared state lives in Redis or the object store, so any service can be replicated without changing behavior.

## Required Services

Students must implement the following services.

### Measurement API Gateway

The Measurement API Gateway is the main entry point for the ping script or external clients.

It exposes HTTP endpoints that allow clients to:

- Submit raw network measurements.

- Request analysis for an already stored measurement.

- Submit raw measurements and request analysis.

- Query the status of submitted jobs.


- Retrieve generated reports as JSON or CSV.

The API Gateway must not directly perform storage, analytics, or report generation. Instead, it should validate incoming requests and delegate to other services via the message broker. It is the only service that speaks HTTP.

**The gateway delegates every domain write.** It must never write a measurement, an analysis, or a report. Those belong to the storage, analytics and report services respectively.

**Job lifecycle record.** The gateway must create the job and save its record before publishing to the message broker. Therefore, newly created jobs can be returned immediately and can be queried by the client to check completion.

**No state in the gateway.** The job record is the gateway's only state and lives in Redis; two replicas must look identical to a client (see the architectural constraints above).

#### Required endpoints

At minimum, the API Gateway should provide:

    POST /measurements
    POST /measurements/analyze
    POST /measurements/{measurement_id}/analyze
    GET  /jobs/{job_id}
    GET  /measurements/{measurement_id}
    GET  /reports/{report_id}
    GET  /health

`GET /reports/{report_id}` returns JSON by default. Adding the query parameter `?format=csv` returns the same report rendered as CSV instead. It is a second representation of one report, not a second report.

#### Responsibilities

The API Gateway should:

- Accept JSON requests from clients.

- Validate required fields.

- Create unique identifiers such as:

  - `job_id`

  - `measurement_id`

- Publish messages to RabbitMQ.

- Return an immediate response to the client.

- Provide a way to query job status and results.

### Measurement Storage Service

The Measurement Storage Service consumes storage jobs from RabbitMQ and stores raw measurement data.

It should consume messages from:

    measurement.created

#### Responsibilities

The Measurement Storage Service should:

- Consume measurement storage jobs from RabbitMQ.

- Store raw measurement data in the object store.

- Save measurement metadata, such as:

  - `measurement_id`

  - source host

  - target host

  - metric type

  - timestamp

  - object path (S3 Storage)

  - measurement status

- Mark the storage job as completed, if measurement is only stored.

- Optionally publish a follow-up message if analytics was requested and mark the job as stored (not completed).

The storage service owns measurement metadata. It is the only service that writes it.

#### Example object path

    measurements/h1-h2/m-001.json

### QoS Analytics Service

The QoS Analytics Service consumes analytics jobs from RabbitMQ.

It should consume messages from:

    measurement.analysis.requested

#### Responsibilities

The QoS Analytics Service should:

- Retrieve raw measurement data from the object store.

- Compute latency statistics.

- Detect simple network anomalies.

- Mark the job `analyzed`, or `failed_analysis` if the analysis cannot be produced.

- Publish a completion message to the report service.

#### Required analytics

Students should implement at least two of the following analytics functions:

- Average latency calculation.

- Minimum and maximum latency calculation.

- Jitter estimation.

- Threshold-based anomaly detection.

- Packet loss percentage, computed only when the client supplied both packet counts.

- Simple quality classification, for example:

  - `nominal`

  - `degraded`

  - `critical`

**Classification rule**: `critical` means an anomaly was detected (SLA violated). Otherwise `nominal` means the average sits at or below 60% of `threshold_ms` (comfortable margin) and any measured packet loss is zero; anything else is `degraded`.

#### Example analytics result

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

`measurement_id`, `metric` and `sample_count` are always present. Every other field belongs to an
analytics function, so it appears only when you implemented that function: `jitter_ms` for jitter,
`packet_loss_percent` when the request carried both packet counts. The later examples on this page
show a job or a report and leave out the fields their workflow did not produce.

#### Simple analytics explanations

Everything starts from a batch of raw latency samples $v_1, v_2, \dots, v_n$ in milliseconds, pulled straight from the stored measurement. The service reduces that batch to a small set of QoS indicators, then maps those indicators onto an SLA *state*. In the following we define the analytics calculations precisely.

**Average, minimum, maximum**

$$\bar{v} = \frac{1}{n}\sum_{i=1}^{n} v_i, \qquad
  v_{\min} = \min_i v_i, \qquad
  v_{\max} = \max_i v_i$$

The average is rounded to one decimal place before being placed in the result: $17.44$ becomes $17.4$, $17.45$ becomes $17.5$. The bounds (`min_latency_ms`, `max_latency_ms`) round to whole milliseconds.

**Jitter: mean absolute successive difference** Jitter is not variance and it is not standard deviation. It measures how much *consecutive* samples vary from one another, computed as the mean of the absolute differences between adjacent samples:

$$J = \frac{1}{n-1}\sum_{i=2}^{n} \left| v_i - v_{i-1} \right|$$

Two batches can share the same mean and the same variance and still have very different jitter. Take `[10, 30, 10, 30, 10]`: it alternates hard, every adjacent pair swings 20 ms. Compare `[10, 10, 10, 30, 30]`: it drifts once and then holds steady, so only one of the four gaps actually jumps. Variance scores these two batches as roughly the same kind of spread; jitter does not, because a voice call does not stutter from a high *average* delay, it stutters when the delay changes unpredictably between two consecutive packets. That is the whole reason jitter is computed pairwise instead of against the mean.

A batch with one sample has no adjacent pair to compare, so $J = 0$ by definition in that case.

#### Threshold calculation

> `threshold_ms` is the SLA ceiling for the latency samples, in milliseconds. A batch **breaches** that ceiling, and is flagged `anomaly_detected = true`, the moment its single worst sample exceeds it:

$$
\mathrm{anomaly}_{\mathrm{detected}} \iff v_{\max} > \mathrm{threshold}_{\mathrm{ms}}
$$

Packet loss has its own ceiling: loss above **5%** is an anomaly as well, because drops and delay are different failure modes and one shared number could not say which happened. Either condition alone is enough.

One breached sample is enough to fail the whole batch; the average plays no part in this test, only in the state classification below. A single 300 ms spike hidden inside an otherwise fine average is exactly what this check exists to catch: testing the mean instead would let that spike hide behind four good samples and pass the batch as fine, and for a real-time flow, that one spike is what a user actually felt, not the session's average.

If a request omits `threshold_ms`, the service falls back to a default of **30 ms**.

**Packet loss** is computed only when both counts arrived:

$$\mathrm{loss}_{\%} = \frac{\mathrm{sent} - \min(\mathrm{received}, \mathrm{sent})}{\mathrm{sent}} \times 100$$

A missing count is not zero loss. "We do not know" and "none was lost" are different claims, so a result with no counts carries no loss field at all.

#### From numbers to an SLA state

We classify the measurements similarly to how a network operations dashboard would show them:

| State | Meaning | Condition |
|---|---|---|
| **`nominal`** | Operating normally, comfortable margin to the SLA ceiling | see below |
| **`degraded`** | Inside the SLA, but eroding toward it | see below |
| **`critical`** | SLA violated: an anomaly was detected | `anomaly_detected = true` |

Formally:

$$\text{state} =
  \begin{cases}
    \texttt{critical} & \text{if } \mathrm{anomaly}_{\mathrm{detected}} \\[4pt]
    \texttt{nominal}  & \text{if } \bar{v} \le 0.6 \times \mathrm{threshold}_{\mathrm{ms}} \text{ and } (\mathrm{loss} = 0 \text{ or loss is unavailable}) \\[4pt]
    \texttt{degraded} & \text{otherwise}
  \end{cases}$$

Any nonzero packet loss keeps a batch out of **`nominal`**, even well under the 5% anomaly ceiling. A flow that drops packets is not operating normally, however good its average delay looks.

If packet-loss information is unavailable, the classification is determined using the available latency metrics. Missing packet-loss information must not be interpreted as actual packet loss.

The $0.6$ factor is headroom, not padding. A batch sitting at 60% of the SLA ceiling still has a third of its budget left, so it counts as genuinely normal operation rather than a borderline pass. A batch averaging 25 ms against a 30 ms ceiling has not breached anything, but it has almost nothing left in reserve; that is **`degraded`**, because the next reading could tip it over.

### Report Service

The Report Service builds and stores reports. A **report** is a retrievable artifact derived from a completed analysis; the **analysis** itself is produced by the QoS Analytics Service.

It should consume messages from:

    analysis.completed

#### Responsibilities

The Report Service should:

- Consume finished analyses from `analysis.completed`.

- Derive the `report_id` from the `measurement_id`.

- Store a report document, analysis included, in the `reports` bucket.

- Mark the job `completed`, or `failed_report_storage` if the write fails.

- Make reports retrievable by `report_id`, in JSON or CSV.

#### Reports reference their analysis

A report document must **embed** its analysis result. Both representations, JSON and CSV, are produced from the report that is stored in the `reports` bucket at the time the report is requested.

**Example report result**

    {
      "report_id": "r-001",
      "job_id": "job-001",
      "measurement_id": "m-001",
      "result": {
        "measurement_id": "m-001",
        "metric": "latency",
        "unit": "ms",
        "sample_count": 5,
        "average_latency_ms": 17.4,
        "min_latency_ms": 9,
        "max_latency_ms": 45,
        "anomaly_detected": true,
        "quality": "critical"
      }
    }

Retrieving a report as CSV:

    GET /reports/{report_id}?format=csv

Expected example CSV response, with `Content-Type: text/csv`:

    measurement_id,metric,unit,sample_count,average_latency_ms,min_latency_ms,
    max_latency_ms,anomaly_detected,quality
    m-001,latency,ms,5,17.4,9,45,true,critical

#### Report identifiers

The `report_id` must be **derived from the `measurement_id`**, not randomly generated. For example, measurement `m-a1b2` always produces report `r-a1b2`.

Deriving from the *measurement* rather than from the job goes further: a report's entire content comes from that measurement's analysis, so every job that analyses the same measurement writes the same report object. A measurement has exactly one report, no matter how often it is analyzed. The `job_id` recorded on the report names the job that most recently produced it.


#### Example report object path

    reports/r-001.json

### RabbitMQ

RabbitMQ is used for asynchronous communication between services.

At minimum, students must define the following queues:

    measurement.created
    measurement.analysis.requested
    analysis.completed

Optional but recommended queues:

    measurement.failed

*Note: the error queue has **no consumer** in this assignment and posting errors to an error queue is optional.*

Each queue is named for the *event* that puts work on it rather than for the service that drains it, so the message flow can be read straight off the queue declarations.

#### What is published and consumed

| Queue | Consumed by | Published by |
|---|---|---|
| `measurement.created` | Storage Service | API Gateway |
| `measurement.analysis.requested` | QoS Analytics | Storage Service, API Gateway |
| `analysis.completed` | Report Service | QoS Analytics |
| `measurement.failed` | nobody | any service, optionally |

`measurement.analysis.requested` is the one queue with two producers. The Storage Service publishes to it after writing a measurement that also asked for analysis (Workflow B), and the API Gateway publishes to it directly for a measurement stored earlier (Workflow C). Both are the same request for the same work, so both arrive at the same place.

#### Purpose of RabbitMQ

- The API Gateway does not wait for long-running processing.

- Storage, analytics, and report generation can be performed asynchronously.

- Services are loosely coupled.

- Multiple workers can consume jobs from the same queue.

- The system can handle multiple requests from the ping script.

**[At-least-once delivery](https://www.rabbitmq.com/docs/confirms) must be handled.** RabbitMQ guarantees that a message is delivered *at least* once, not exactly once. A consumer that crashes after doing its work but before acknowledging will see the same message again. Network hiccups and broker restarts cause the same thing. Students must assume every consumer can run twice on the same message.

**Requirement.** Processing a message twice must not produce a second artifact. Concretely, requesting analysis or report and forcing a redelivery must not leave two analyses for one measurement, or two reports for one measurement.

**One queue in, one queue out.** Each consuming service subscribes to exactly one queue and may publish to at most one queue. A service must not consume from two queues, must not publish to a queue it also consumes from, and must not publish to several queues at once. ***Exception:** error queues.*

This constrains the three *consumers*. The API Gateway consumes nothing, so it is free to publish to both `measurement.created` and `measurement.analysis.requested`, one per workflow it accepts.

This keeps the system a pipeline whose stages can be reasoned about, replicated and restarted independently. It also means the message flow can be read straight off the queue declarations.

### Object Storage (Garage)

An S3-compatible object store is used for raw measurements and generated reports. For this assignment, students should deploy a single **Garage** instance, and use a provider-agnostic S3 client library to read/write to storage.

Students should create the following buckets:

    measurements
    reports

Recommended additional buckets:

    errors

#### Stored objects

The object store should hold:

- Raw measurement JSON files.

- Report documents.

- *Optional* error records.

The current analysis is stored once, embedded in its report. A job record references that report; nothing copies the analysis.

### Redis

Redis holds the shared, mutable state of the pipeline so that every service can remain stateless and replicated.

Students should store:

- Job state, keyed for example as `job:<job_id>`.

- Measurement metadata, keyed for example as `meas:<measurement_id>`.

#### Field-level updates

Job state should be stored as a **hash**, and each service should update only the fields it owns rather than rewriting the whole record. The storage and analytics services advance the status; the report service writes the report identifier.

If each service instead read the whole job document, modified one field and wrote it back, two services completing at nearly the same time would overwrite each other's fields. Writing individual fields removes that class of lost update without needing a lock.

#### Example job hash

    job:job-001
      measurement_id  m-001
      status          completed
      report_id       r-001

Note that the job record holds a *reference* to its report rather than a copy of it. The API Gateway joins the analysis into its responses when serving `GET /jobs/{job_id}` and `GET /reports/{report_id}`.

#### Persistence

State storage must not be ephemeral. Redis must be started with persistence enabled (append-only mode) and backed by a volume; the object store must likewise be backed by a volume. Restarting the stack, or restarting a single container, must not lose submitted jobs, stored measurements, analyses or reports.

A default in-memory Redis with no volume does not satisfy this, even though the system will appear to work in a short demo. The job hash should contain the job status and, once a report exists, the derived `report_id`. It should **not** contain a copy of the analysis result, which is stored once inside the report document and joined into API responses on read.

## Required Workflows

### Job Life-cycle

The three workflows are three paths through one state machine. Which statuses a job passes through is determined by how much work was asked for, and every status names a pipeline stage:

::::{container} centered
```{mermaid}
stateDiagram-v2
    direction LR

    [*] --> QUEUED

    QUEUED --> STORED
    QUEUED --> ANALYZED
    QUEUED --> COMPLETED

    STORED --> ANALYZED
    ANALYZED --> COMPLETED
    COMPLETED --> [*]

    state Failures {
        FAILED_MEASUREMENT_STORAGE
        FAILED_ANALYSIS
        FAILED_REPORT_STORAGE
    }

    QUEUED --> FAILED_MEASUREMENT_STORAGE
    QUEUED --> FAILED_ANALYSIS
    STORED --> FAILED_ANALYSIS
    ANALYZED --> FAILED_REPORT_STORAGE
```
::::

| Status | Meaning |
|---|---|
| `queued` | Accepted by the gateway, message on a queue |
| `stored` | Raw measurement written; analysis still to come |
| `analyzed` | Analysis computed and handed to the report service |
| `completed` | Terminal success: the job's last stage finished |
| `failed_measurement_storage` | The storage service could not write the measurement |
| `failed_analysis` | The analytics service could not produce a result |
| `failed_report_storage` | The report service could not write the report |

Students must support the following workflows.

### Workflow A: Store Measurement Only

The ping script sends network measurement data to the API Gateway.

::::{container} centered
```{mermaid}
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant R as Redis
    participant Q as RabbitMQ
    participant S as Storage

    C->>G: POST /measurements
    G->>R: create job (queued)
    G->>Q: publish storage job
    G-->>C: 202 job_id, queued
    Q->>S: deliver
    Note over S: writes the raw measurement<br/>to the object store
    S->>R: status = completed
```
::::

The storage service writes the raw measurement to the object store before it marks the job completed.

Example request:

    POST /measurements
    Content-Type: application/json

Example body:

    {
      "source_host": "h1",
      "target_host": "h2",
      "metric": "latency",
      "values_ms": [10, 11, 13, 12, 15],
      "timestamp": "2026-06-22T10:00:00Z"
    }

The **API Gateway** should:

1.  Accept the measurement data.

2.  Validate the request.

3.  Create a unique `measurement_id`.

4.  Create a unique `job_id`.

5.  Record the job in Redis with status `queued`.

6.  Publish a message to `measurement.created`.

7.  Return a response to the client.

The **Measurement Storage Service** should:

1.  Consume the storage message.

2.  Store the measurement data in the object store.

3.  Save the measurement metadata in Redis.

4.  Mark the job as completed.

Expected response:

    {
      "job_id": "job-001",
      "measurement_id": "m-001",
      "status": "queued"
    }

Expected final job status:

    {
      "job_id": "job-001",
      "measurement_id": "m-001",
      "status": "completed"
    }

### Workflow B: Store Measurement and Analyze It

The ping script sends measurement data and requests analysis. This is the full chain: every service takes part, and each hands off through RabbitMQ rather than calling the next one directly.

::::{container} centered
```{mermaid}
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant Q as RabbitMQ
    participant S as Storage
    participant A as Analytics
    participant P as Report

    C->>G: POST /measurements/analyze
    G->>Q: storage job (analyze after)
    G-->>C: 202 job_id, queued
    Q->>S: deliver
    Note over S: writes the raw measurement,<br/>status = stored
    S->>Q: analytics job
    Q->>A: deliver
    Note over A: computes the analysis and<br/>hands it over, persists nothing
    A->>Q: report job (carries the analysis)<br/>analysis.completed
    Q->>P: deliver
    Note over P: derives r-x from m-x,<br/>stores the report with its analysis,<br/>marks the job completed
```
::::

Storage writes the raw measurement. Analytics computes the analysis and hands it to the Report Service on `analysis.completed`; it stores nothing itself. Report derives `r-x` from `m-x`, stores the report document with the analysis embedded, and marks the job `completed`.

Example request:

    POST /measurements/analyze
    Content-Type: application/json

Example body:

    {
      "source_host": "h1",
      "target_host": "h3",
      "metric": "latency",
      "values_ms": [10, 11, 9, 45, 12],
      "analysis": "latency_summary",
      "threshold_ms": 30
    }

The **API Gateway** should:

1.  Validate the measurement data and requested analysis.

2.  Accept the request.

3.  Create a `measurement_id` and `job_id`.

4.  Record the job in Redis with status `queued`.

5.  Publish a storage message to `measurement.created`.

6.  Include in the message that analysis is requested after storage.

7.  Return a job ID to the client.

The **Measurement Storage Service** should:

1.  Store the raw measurement in the object store.

2.  Mark the storage step as stored.

3.  Publish a new message to `measurement.analysis.requested`.

The **QoS Analytics Service** should:

1.  Retrieve the stored measurement from the object store.

2.  Run the requested analysis.

3.  Detect whether an anomaly exists.

4.  Mark job as analyzed.

5.  Publish a completion message to `analysis.completed`.

The **Report Service** should:

1.  Consume the completion message.

2.  Derive the `report_id` from the `measurement_id`.

3.  Store a report document in the `reports` bucket.

4.  Mark the job as completed.

Expected final result:

    {
      "job_id": "job-002",
      "measurement_id": "m-002",
      "status": "completed",
      "report_id": "r-002",
      "result": {
        "average_latency_ms": 17.4,
        "min_latency_ms": 9,
        "max_latency_ms": 45,
        "threshold_ms": 30,
        "anomaly_detected": true,
        "quality": "critical"
      }
    }

### Workflow C: Analyze an Already Stored Measurement

A client requests analysis for a measurement that was already stored earlier. The storage service takes no part: the raw measurement is already in the object store.

::::{container} centered
```{mermaid}
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant Q as RabbitMQ
    participant A as Analytics
    participant P as Report

    C->>G: POST /measurements/{id}/analyze
    Note over G: 404 first if the<br/>measurement does not exist
    G->>Q: analytics job
    G-->>C: 202 job_id, queued
    Q->>A: deliver
    Note over A: computes the analysis and<br/>hands it over, persists nothing
    A->>Q: report job (carries the analysis)<br/>analysis.completed
    Q->>P: deliver
    Note over P: derives r-x from m-x,<br/>stores the report with its analysis,<br/>marks the job completed
```
::::

The gateway responds 404 before creating a job if the measurement does not exist.

Example request:

    POST /measurements/{measurement_id}/analyze
    Content-Type: application/json

Example body:

    {
      "analysis": "latency_summary",
      "threshold_ms": 25
    }

The **API Gateway** should:

1.  Check whether the `measurement_id` exists.

2.  Create a new `job_id`.

3.  Publish an analysis message to `measurement.analysis.requested`.

4.  Return the job ID to the client.

The **QoS Analytics Service** should:

1.  Retrieve the existing measurement from the object store.

2.  Run the requested analysis.

3.  Mark the job as analyzed.

4.  Publish a completion message.

Expected final result:

    {
      "job_id": "job-003",
      "measurement_id": "m-001",
      "status": "completed",
      "report_id": "r-001",
      "result": {
        "average_latency_ms": 12.2,
        "min_latency_ms": 10,
        "max_latency_ms": 15,
        "threshold_ms": 25,
        "anomaly_detected": false,
        "quality": "nominal"
      }
    }

## Containerization and Orchestration

### Docker and Docker Compose Requirements

Students must provide a `docker-compose.yml` file that starts the complete system.

The system must be startable with:

    docker compose up --build

The Compose file must start the four application services together with RabbitMQ, the S3 object store, and Redis.

**One process per container, one image per service.** Each of the four services gets its own image and its own container running a single process. A shared build stage that produces four distinct images is fine; a single image with four roles is not.

**Named volumes are required.** Both Redis and the object store must mount a named volume. `docker compose down` followed by `up` must preserve previously submitted jobs and stored measurements. (`down -v` deletes volumes by design and is the correct way to reset.)

### Kubernetes Deployment

Students must also deploy the same application using Kubernetes. For this assignment we recommend using **[Minikube](https://minikube.sigs.k8s.io/docs/)**, as it is lightweight and best suited for local development.

The Kubernetes deployment should include:

- [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) for Measurement API Gateway

- Deployment for Measurement Storage Service

- Deployment for Report Service

- Deployment for QoS Analytics Service

- Deployment or [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) for RabbitMQ

- Deployment or StatefulSet for S3 object store

- Deployment or StatefulSet for Redis

**Required Kubernetes Resources.** Students must provide either Kubernetes **[manifests](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/)** or a **[Helm](https://helm.sh/docs/)-based deployment**.

If using manifests, the repository should include files such as:

    k8s/manifests/
      00-namespace.yaml
      05-config.yaml          # ConfigMaps: shared env, garage.toml
      06-secret.yaml          # S3 and RabbitMQ credentials
      10-garage.yaml          # PVCs + Deployment + Service
      15-redis.yaml           # PVC + Deployment + Service
      20-rabbitmq.yaml        # Deployment + Service
      30-api-gateway.yaml     # Deployment + NodePort Service
      40-storage-service.yaml # Deployment only: nothing dials it
      50-analytics-service.yaml
      60-report-service.yaml

A Helm-based deployment is equivalent. The [Chart](https://helm.sh/docs/topics/charts/) can be laid out as:

    k8s/helm/measurement-pipeline/
      Chart.yaml                    # chart metadata (name, version)
      values.yaml                   # images, replica counts, credentials, sizes
      templates/
        _helpers.tpl                # shared name/label helpers
        NOTES.txt                   # post-install usage hints
        config.yaml                 # ConfigMap: shared env, garage.toml
        secret.yaml                 # Secret: S3 and RabbitMQ credentials
        garage.yaml                 # PVCs + Deployment + Service
        redis.yaml                  # PVC + Deployment + Service
        rabbitmq.yaml               # Deployment + Service
        services.yaml               # the 4 app Deployments + API-Gateway NodePort

Each file holds every object belonging to one component, and the numeric prefixes give `kubectl apply` a sensible order. Among the four application services, only the API Gateway requires a Kubernetes Service. Infrastructure components such as Redis, RabbitMQ, and Garage also require internal Kubernetes Services. The three application consumers pull work from RabbitMQ, so they do not require their own Services.

Redis and the object store each need a [`PersistentVolumeClaim`](https://kubernetes.io/docs/concepts/storage/persistent-volumes/). Such a volume is `ReadWriteOnce`, so only one node may mount it at a time. Think about what a [rolling update](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy) does: it brings a new pod up *before* taking the old one down, and for a moment both want the same volume. Students should work out which update strategy avoids that clash and configure their deployments accordingly.

**Replication Requirement.** At least one stateless service must run with more than one replica.

Suitable services for replication:

- Measurement API Gateway

- Measurement Storage Service

- QoS Analytics Service

- Report Service

RabbitMQ, the object store, and Redis should usually run with one replica unless students intentionally design a clustered setup.

Example:

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: analytics-service
    spec:
      replicas: 2

Because all state objects live in shared storage, running several replicas of a service must not change the observable behavior of the system.

(testing-requirements)=
## Testing Requirements

Students must provide evidence that their system works.

The report documents the tests and their results across API behavior, Docker Compose, and Kubernetes. Include the screenshots, logs, bucket listings, and command outputs requested below in a traceable, readable form. Also show a request from the template's ping script.

### Required API Behavior

The API does not need to be complex, but it must support job-based asynchronous processing.

When a client submits a request, the API should return quickly with a job ID.

**Asynchronous means the response does not wait.** Every write endpoint must return as soon as the message is on the queue, carrying a `job_id` and the status `queued`. The client learns the outcome by polling `GET /jobs/{job_id}`.

An endpoint that blocks until the work is finished and then returns the finished result is a synchronous API wearing a job id, and does not satisfy this assignment even if the response body looks correct.

**Processing the same message twice must not create duplicate artifacts.** Analyzing the same measurement twice must leave the `report_id` unchanged, and the `reports` bucket must list one report object rather than two.

Example initial response:

    {
      "job_id": "job-100",
      "measurement_id": "m-100",
      "status": "queued"
    }

The client should then be able to query:

    GET /jobs/job-100

Example job response while processing:

    {
      "job_id": "job-100",
      "status": "stored"
    }

Example job response after completion:

    {
      "job_id": "job-100",
      "measurement_id": "m-100",
      "status": "completed",
      "report_id": "r-100",
      "result": {
        "average_latency_ms": 17.4,
        "anomaly_detected": true
      }
    }

**Error responses.** The API should use the status code that matches the failure:

- **400** for an invalid request body.

- **404** for an unknown measurement, job, or report.

- **503** when a message cannot be published to RabbitMQ.

If the gateway records a job but then fails to publish its message, it must remove that job again rather than leaving it stuck in `queued` forever.

### RabbitMQ Test

Show that messages pass through RabbitMQ. Evidence may include a RabbitMQ management UI screenshot, producer and consumer logs, queue length changes, or consumer acknowledgement logs.

### Object Storage Test

Show that the object store contains the raw measurement JSON and the report document. Evidence may include a console screenshot or bucket listing, downloaded copies of both objects, or storage service logs.

Also show that the `reports` bucket contains **no** CSV objects: the CSV representation is rendered on retrieval, not stored.

### Redis and Durability Test

Show that job and measurement state is held in Redis. Evidence may include:

    redis-cli KEYS "job:*"
    redis-cli HGETALL job:job-001

Inspect the job hash to show its status and, once a report exists, the derived `report_id`. Confirm that it does **not** contain a copy of the analysis result.

Demonstrate durability explicitly: submit a job, restart the container, and show that the job is still queryable.

### Local Docker Compose Test

Students should show:

    docker compose up --build
    docker compose ps

They should submit example requests using `curl` or a Python client.

Required tests:

1.  Submit a measurement.

2.  Submit a measurement with analysis.

3.  Analyze an existing measurement.

4.  Query a job status.

5.  Generate a report and retrieve it as JSON and as CSV.

### Kubernetes Test

Students should show:

    kubectl apply -f k8s/manifests/
    kubectl get pods -n aic
    kubectl get deployments -n aic
    kubectl get services -n aic
    kubectl logs -n aic <pod-name>

A Helm-based deployment is equivalent:

    helm install measurement-pipeline k8s/helm/measurement-pipeline -n aic --create-namespace

The two are alternatives, not complements: they create the same objects under the same names in the same namespace, so only one may be installed at a time.

They should demonstrate that:

- All required services start successfully.

- At least one service has multiple replicas.

- Services communicate through Kubernetes Services.

- The API Gateway is reachable.

- A request can be processed end-to-end.

The gateway is published on a `NodePort`, but whether the host can route to the node's address depends on the Kubernetes driver and may stop working after a restart even while the cluster is healthy. Port-forwarding goes through the API server instead and is therefore the reliable way to reach the gateway:

    kubectl port-forward -n aic svc/api-gateway 18080:8080
    curl -s http://127.0.0.1:18080/health

If a request hangs, check the service from inside the node with `minikube ssh "curl -s localhost:30080/health"` first: that separates a broken deployment from a host routing problem.

(interview-grading)=
## Interview & Grading

The exercise is scored out of 100 points in two parts.

| Part | Weight |
|---|--------|
| Interview | 50p    |
| Assignment | 50p    |

The deadline comes from the course announcement, not this page.

### Assignment, 50p

The assignment score is the submitted system. The four buckets below add up to its 50 points.

| Bucket | Share |
|---|-------|
| Required Services | 20p   |
| Required Workflows | 10p   |
| Containerisation and Orchestration | 5p    |
| Testing Requirements | 15p   |



### Interview, 50p

The total 50 points are split between the interview and the report.

| Bucket                             | Share |
|------------------------------------|-------|
| Interview                          | 40p   |
| Report                             | 10p   |


The interview checks that you understand the system you built. You should be able to explain the technologies used, how the pipeline works, and the decisions made in your implementation, such as why a queue sits between two stages and where each piece of state lives.

Students should understand RabbitMQ, Redis, object storage, asynchronous processing, stateless services, service decomposition, persistence, idempotency, and the differences between Docker Compose and Kubernetes. Expect questions on any of them.

Students should be able to explain the working system, failing to do so can result in 0 points for the **ENTIRE** assignment.


#### Report

The interview also checks your report. It should document the system you built and the evidence required in {ref}`Testing Requirements <testing-requirements>`.
The report should be no more than 10 pages long.

Output should be documented in a traceable and readable way, failing to do so can result in 0 points for the report.

### Deliverables

Hand in the source of all four services together with:

- A `docker-compose.yml` that starts the whole stack.

- Kubernetes manifests or a Helm chart.

- A `README.md` with instructions to start the project and run the tests.

- Whatever configuration the backing services need, such as environment files, bucket setup, and queue declarations.

- The **report**, with the logs, screenshots, or command outputs that back its claims.

One command must start the project. `docker compose up --build` is enough. A `Makefile` that wraps the commands is recommended, but optional.

The submission keeps the structure of the {ref}`starting template <starting-template>`:

```{raw} html
<a class="download-card" href="_static/measurement-pipeline-template.zip" download>
  <span>
    <span class="download-card-title">Starting template (ZIP, 40&nbsp;KB)</span>
    <span class="download-card-meta">Service skeleton, ping script, Bruno collection, empty Compose and Kubernetes files</span>
  </span>
</a>
```

The layout below is a Python example of how a submission should look. It is not mandatory, but a submission that follows it is quick to review.

    measurement-pipeline/
      docker-compose.yml          # starts the whole stack
      Makefile                    # optional: wraps the common commands
      README.md                   # how to start the stack and run the tests
      .env.example                # endpoints and credentials, no real secrets
      services/
        Dockerfile                # multi-stage: four targets, four images
        requirements.txt
        pyproject.toml
        aic_common/               # shared code only, no service logic
          contract.py             # the fixed names: queues, buckets, statuses, result fields
        api_gateway/              # the only service that speaks HTTP
          main.py
        storage_service/
          main.py
        analytics_service/
          main.py
          qos.py                  # average, jitter, threshold, classification
        report_service/
          main.py
        tests/                    # unit tests, no running stack needed
      k8s/
        manifests/                # or helm/, not both
      s3/
        garage.toml               # object store configuration
      scripts/
        measurement_simulator.py  # the ping simulator from the template
        simulator.yaml            # its hosts, rounds and latency range
      tests/                      # end-to-end tests against a running stack
      evidence/                   # logs, screenshots, command outputs
      report.pdf

Each service directory is a Python package and holds one service. Shared code lives in `aic_common` and holds none.
