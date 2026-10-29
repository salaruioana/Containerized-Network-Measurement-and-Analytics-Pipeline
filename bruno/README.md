# Measurement API, Bruno Collection

A small [Bruno](https://www.usebruno.com/) collection to send requests to a running Measurement API
Gateway by hand. It is a testing aid only and contains no solution code. For continuous traffic, use
`scripts/measurement_simulator.py`.

## Use

1. Open this folder as a collection in Bruno.
2. Select an environment. **Local (docker compose)** is `http://localhost:8080`. **Minikube
   (NodePort)** is `http://<minikube-ip>:30080`. Set the IP to your own.
3. Send the requests in this order:
   - **Health** checks that the gateway is up.
   - **Submit Measurement (Workflow A)** stores a measurement.
   - **Submit and Analyze (Workflow B)** stores a measurement and analyses it.
   - **Analyze Stored Measurement (Workflow C)** analyses the measurement from the last submit.
   - **Get Job**, **Get Measurement** and **Get Report** get the results of the last `POST`. Send
     **Get Job** again to see the status change.

Each `POST` with a 202 response saves `job_id` and `measurement_id` in runtime variables. It also
derives `reportId` from the measurement id. Workflow C and the `Get` requests read these variables,
so you do not copy ids by hand. To get an older job, type its id into the path param.

Each workflow ends at `completed`. Workflow A reaches it when the measurement is stored. Workflow B
goes through `stored` and `analyzed` first. Workflow C skips `stored`, because the measurement is
already stored. At the end of B and C, the job has a `report_id` and a `result`. A failed job stops
at the status of the stage that failed: `failed_measurement_storage`, `failed_analysis` or
`failed_report_storage`.

## Errors

The **Errors** folder has 4 requests that the contract says must fail: 2 invalid bodies (400), an
unknown job (404), and Workflow C on a measurement that does not exist (404). None of them may
create a job.

## Assertions and the Runner

Each request asserts the status code from the contract. For a quick smoke test, run the whole
collection with the Bruno **Runner**, or from the command line:

```bash
npx @usebruno/cli run --env "Local (docker compose)"
```

A green run does not show that a job reached `completed`. The pipeline is asynchronous, and the
Runner gets the job directly after the submit. To see `completed`, send **Get Job** again by hand.
On the unchanged template, only **Health** and one 400 request pass. All other routes return 501.

## Expected Numbers

The sample values come from the worked examples in the assignment. Compare your output with the
numbers there:

- **Workflow B** sends `[10, 11, 9, 45, 12]` with a threshold of 30. Expect an average of 17.4, a
  min of 9, a max of 45, and an anomaly, because the worst sample is above the threshold.
- **Workflow A** stores `[10, 11, 12, 13, 15]`. Send **Workflow C** directly after it, with a
  threshold of 25. Expect an average of 12.2, a min of 10, a max of 15, and no anomaly. If you send
  B between them, C analyses the measurement from B.

The fields in the result depend on the analytics functions you implemented. To simulate other
measurements, change the hosts, the metric and the values.
