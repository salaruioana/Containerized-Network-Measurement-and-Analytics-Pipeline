# Division of work between five people

Each person owns one work package, including implementation, unit tests where applicable, service-specific debugging and evidence of their contribution.

## Person 1 - API Gateway (Yaron)

*Main responsibility:* HTTP API, validation, job creation and message publishing.

### Implementation
- Implement the six required endpoints:
  - POST /measurements
  - POST /measurements/analyze
  - POST /measurements/{measurement_id}/analyze
  - GET /jobs/{job_id}
  - GET /measurements/{measurement_id}
  - GET /reports/{report_id}
- Keep the existing /health endpoint.
- Validate incoming measurement requests and required fields.
- Generate job and measurement IDs.
- Create job records in Redis before publishing messages.
- Publish messages to the correct RabbitMQ queues.
- Remove the newly created job if publishing fails and return an appropriate error.
- Verify that a requested measurement exists before creating a job for Workflow C.
- Retrieve measurements, jobs and reports from the correct data sources.
- Generate CSV output from report data when ?format=csv is requested.

### Tests and deliverables
- API tests for valid and invalid requests.
- Tests for 202, 400, 404 and queue-publishing failure behavior.
- Tests for job, measurement and report retrieval.
- A working API Gateway container and a short explanation of its endpoints.

### Dependencies
Needs agreed message formats, Redis job conventions, measurement metadata format and report format. Can develop request models and validation before all services are ready.

## Person 2 - Storage Service (Sebastian)

*Main responsibility:* raw measurement persistence, measurement metadata and the first workflow transition.

### Implementation
- Consume measurement.created.
- Validate or safely handle incoming messages according to the agreed contract.
- Write raw measurement data to the Garage measurements bucket.
- Store measurement metadata in Redis.
- Update the job status using field-level updates.
- For Workflow A, mark the job completed.
- For Workflow B, mark the job stored and publish measurement.analysis.requested.
- Handle failed storage operations by setting failed_measurement_storage and logging the job ID.
- Make message processing idempotent so redelivery does not create duplicate artifacts.

### Tests and deliverables
- Tests for successful storage, metadata, job transitions and analysis publishing.
- Tests for failed writes and duplicate messages.
- Evidence that raw measurements exist in Garage and metadata exists in Redis.
- A working Storage Service container and a short description of its processing logic.

### Dependencies
Needs the Gateway's measurement message format and job conventions. Workflow B also depends on the agreed analytics-request message format.

## Person 3 - QoS Analytics Service (Attila)

*Main responsibility:* QoS calculations and processing analysis requests.

### Implementation
- Implement the required functions in analytics_service/qos.py.
- Use the supplied tests as the specification for expected calculations.
- Implement at least two correct analytics functions; the team should agree on the selected metrics and scope.
- Consume measurement.analysis.requested.
- Read the raw measurement from Garage.
- Calculate the requested indicators and classify the result.
- Update the job status to analyzed.
- Publish the result to analysis.completed.
- Handle analysis failures by setting failed_analysis and logging the job ID.
- Do not persist analysis results independently.

### Tests and deliverables
- Make the relevant supplied QoS tests pass.
- Add tests for edge cases, including a single sample, missing packet counts and threshold boundaries.
- Test the consumer with a representative measurement and verify the published result.
- Provide a working Analytics Service container and a short explanation of the implemented metrics.

### Dependencies
The calculation module can be implemented and tested independently. The consumer needs the measurement object format, analysis-request message format and Redis conventions.

## Person 4 - Report Service (Ioana)

*Main responsibility:* report creation, report persistence and final job completion.

### Implementation
- Consume analysis.completed.
- Derive the report ID deterministically from the measurement ID.
- Build a report document containing the analysis result and the agreed report metadata.
- Write the report to the Garage reports bucket.
- Update the job with report_id and status completed.
- Handle failed report writes using failed_report_storage and log the job ID.
- Ensure repeated messages overwrite the same report object instead of creating duplicates.
- Ensure the report's job ID reflects the latest writer if the same measurement is analysed again.

### Tests and deliverables
- Tests for report structure, ID derivation, successful writes and job updates.
- Tests for duplicate messages and failed writes.
- Evidence that report objects exist in Garage and contain the analysis result.
- Evidence that CSV files are not stored as separate objects.
- A working Report Service container and a short description of its behavior.

### Dependencies
Needs the agreed analysis.completed message format and Redis job conventions. Report serialization and ID derivation can be tested independently.

## Person 5 - Infrastructure and Integration Lead (Oliver)

*Main responsibility:* make the complete system deployable and coordinate end-to-end integration.

### Implementation
- Configure Docker Compose for all four application services, RabbitMQ, Redis and Garage.
- Ensure each application service has its own image/container.
- Configure environment variables, networks, health checks and service dependencies.
- Configure Redis append-only persistence and named volumes for Redis and Garage.
- Configure Garage, including its S3 endpoint, credentials and buckets.
- Implement Kubernetes deployment using either plain manifests or Helm.
- Configure the required services, persistent volumes and stateless-service replication.
- Keep the deployment instructions and configuration organized.
- Coordinate integration testing and collect system-level evidence.

### Tests and deliverables
- Demonstrate docker compose up --build launches the complete system.
- Verify persistence after docker compose down and docker compose up.
- Deploy the system to Kubernetes and demonstrate running pods, services, persistent volumes and a replicated stateless service.
- Verify that the Gateway is reachable and that the end-to-end workflows function.
- Maintain deployment instructions in the README and coordinate final evidence collection.

### Dependencies and workload boundaries
This person needs the application owners to provide their ports, environment variables, startup commands and working service interfaces.

Person 5 coordinates infrastructure and integration but does *not* own all application debugging. Each service owner must investigate and fix defects in their own code. Other team members should help diagnose cross-service issues when necessary.

---

# Shared work and fairness

The five parts are not automatically equal in difficulty. The Gateway has many endpoints, Analytics has a relatively independent calculation component, and Infrastructure has the greatest integration uncertainty.

To keep the division fair:

- Every service owner implements and tests their own service.
- Every service owner verifies that their service can run in its container.
- Person 5 leads Compose and Kubernetes but does not implement or repair the other four services.
- Everyone contributes evidence and documentation for their own component.
- Everyone participates in at least one end-to-end test and helps resolve integration problems.
- The team agrees on the message formats and shared interfaces together.
- If infrastructure finishes early, Person 5 can help with integration and testing. If it takes longer than expected, the team should help rather than leave one person blocked.