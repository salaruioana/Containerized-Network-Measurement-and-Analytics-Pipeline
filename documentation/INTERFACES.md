# Shared Interfaces

## Gateway: request validation

The gateway validates every request before creating a job. Invalid requests
return `400` with `{"error": "..."}` and create neither a job nor a message.

| Field | Rule | Reason |
| --- | --- | --- |
| `source_host`, `target_host` | Required, trimmed, 1 to 255 characters, only `[A-Za-z0-9._-]` | Hosts become part of object paths. |
| `metric` | Required, trimmed, 1 to 255 characters, no control characters | Metric names identify the measurement type without carrying control data. |
| `values_ms` (alias `values`) | Required, at least 1 sample; finite numbers `>= 0`; no strings, booleans, `NaN`, or `Infinity` | Samples are latencies in milliseconds; `values_ms` wins if both keys are sent. |
| `threshold_ms` | Optional, finite number `> 0`; the gateway sets no default, analytics falls back to 30 | The analytics service owns the default threshold. |
| `packets_sent`, `packets_received` | Optional, integers `>= 0` with no booleans; independent of each other; `received > sent` is allowed | Packet loss is computed only if both are present; analytics must handle `sent == 0`. |
| `unit`, `analysis` | Optional; if given, not empty after trimming | Optional descriptive values must not be blank. |
| `analysis` | Required for `POST /measurements/analyze` and `POST /measurements/{id}/analyze` | These workflows explicitly request analysis. |
| `timestamp` | Optional; missing means arrival time in UTC; given values are normalized to UTC with suffix `Z`; naive timestamps are treated as UTC; unparsable values are rejected | Queue messages use one normalized timestamp representation. |
| Unknown extra fields | Ignored | Clients such as the simulator may send additional fields. |

### Error format

- Invalid bodies, including malformed JSON, return `400`.
- The body is `{"error": "<field>: <reason>"}`; malformed JSON uses `{"error": "invalid JSON"}`.
- Error text does not contain input values.