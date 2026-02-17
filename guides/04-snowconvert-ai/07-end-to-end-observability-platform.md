# End-to-End Observability Platform

> **Interview Prompt:** "A SnowConvert conversion job fails silently — the output SQL is invalid but no alert fires. Design an observability platform that can trace a single failed SQL conversion across 8 microservices, from API ingestion to output delivery."

---

## 1. Requirements

### Functional
- **Distributed Tracing:** Single trace ID follows a conversion request across all services.
- **Metrics (RED):** Rate, Errors, Duration for every service and endpoint.
- **Structured Logging:** JSON logs with trace ID, service name, and severity correlated with traces.
- **Alerting:** Automated alerts on SLO violations, error rate spikes, and latency anomalies.
- **Dashboards:** Service-level and pipeline-level dashboards for real-time and historical analysis.

### Non-Functional
- **Trace sampling:** 100% of errors traced, 1% of successes (cost control).
- **Metrics retention:** 15 seconds resolution for 30 days, 1-minute resolution for 1 year.
- **Log retention:** 30 days hot, 1 year cold (GCS archive).
- **Alert latency:** < 60 seconds from anomaly to PagerDuty notification.

### Capacity Estimation
```
Services:          8 microservices
Request rate:      5,000 req/sec aggregate
Spans per request: ~15 (avg 2 spans per service)
Trace volume:      5,000 × 15 = 75,000 spans/sec (before sampling)
Sampled:           1% success + 100% errors ≈ 1,500 spans/sec
Span size:         ~500 bytes → 750 KB/sec → ~65 GB/day
Metrics series:    ~50,000 unique time series
Log volume:        ~5 GB/day (structured JSON)
```

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Services (GKE)                   │
│                                                                  │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │API GW   │→│Job Sub  │→│Worker   │→│Validator│              │
│  │         │ │         │ │(Parser) │ │         │              │
│  │ OTel    │ │ OTel    │ │ OTel    │ │ OTel    │              │
│  │ SDK     │ │ SDK     │ │ SDK     │ │ SDK     │              │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘              │
│       │           │           │           │                     │
│  ┌────▼───────────▼───────────▼───────────▼────┐               │
│  │        OpenTelemetry Collector (DaemonSet)    │               │
│  │  - Receives traces, metrics, logs             │               │
│  │  - Tail-based sampling (100% errors, 1% ok)  │               │
│  │  - Batches and exports to backends            │               │
│  └────┬───────────┬───────────┬────────────────┘               │
└───────│───────────│───────────│──────────────────────────────────┘
        │           │           │
   ┌────▼───┐  ┌────▼───┐  ┌───▼────┐
   │Traces  │  │Metrics │  │ Logs   │
   │        │  │        │  │        │
   │Tempo / │  │Promethe│  │Loki /  │
   │Jaeger  │  │us/     │  │Cloud   │
   │        │  │Thanos  │  │Logging │
   └────┬───┘  └────┬───┘  └───┬────┘
        │           │           │
   ┌────▼───────────▼───────────▼────┐
   │          Grafana                 │
   │  - Unified dashboards           │
   │  - Trace → Metrics → Logs       │
   │  - Alerting rules               │
   └─────────────────────────────────┘
```

---

## 3. Deep Dive: The Three Pillars

### 3.1 Distributed Tracing (OpenTelemetry)

```
Trace for a single conversion job:

TraceID: abc123def456

  ┌─ Span: api-gateway.receive_request (12ms)
  │   service: api-gateway
  │   http.method: POST, http.url: /v1/jobs
  │   http.status_code: 202
  │
  ├─ Span: job-submission.create_job (8ms)
  │   service: job-submission
  │   db.operation: INSERT, db.table: jobs
  │   job.id: job-a1b2c3d4
  │
  ├─ Span: pubsub.publish (3ms)
  │   service: job-submission
  │   messaging.system: pubsub, messaging.destination: conversion-jobs
  │
  ├─ Span: worker.process_file (28,400ms)  ← THE BOTTLENECK
  │   service: conversion-worker
  │   file.size_bytes: 2500000
  │   source_dialect: oracle_plsql
  │   │
  │   ├─ Span: worker.parse_ast (12,300ms)
  │   │   ast.node_count: 45000
  │   │   ast.max_depth: 42
  │   │
  │   ├─ Span: worker.transpile (15,800ms)
  │   │   transpile.warnings: 3
  │   │   transpile.unsupported_constructs: ["CONNECT BY", "MODEL"]
  │   │
  │   └─ Span: worker.write_output (300ms)
  │       gcs.bucket: outputs, gcs.object_size: 1800000
  │
  └─ Span: validator.validate (2,100ms)
      service: validator
      validation.method: snowflake_explain
      validation.result: PASS
      validation.warnings: 1
```

**Context Propagation:**
```
Service A calls Service B:
  HTTP Header: traceparent: 00-abc123def456-span789-01
  
  Service B extracts trace ID from header
  Creates child span with same trace ID
  Propagates to Service C in the same way

For async (Pub/Sub):
  trace_id and span_id are embedded in message attributes:
  message.attributes["traceparent"] = "00-abc123def456-span789-01"
```

### 3.2 RED Metrics

```
Rate (request throughput):
  http_requests_total{service="api-gateway", method="POST", endpoint="/v1/jobs"}

Error rate:
  http_requests_total{service="api-gateway", status_code=~"5.."}
  / http_requests_total{service="api-gateway"}

Duration (latency):
  http_request_duration_seconds{service="api-gateway", endpoint="/v1/jobs"}
  Histogram buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300]

Custom business metrics:
  conversion_jobs_total{dialect="oracle", status="completed"}
  conversion_duration_seconds{dialect="oracle", file_size_bucket="large"}
  conversion_accuracy_score{dialect="oracle"}
  dlq_depth{error_class="WORKER_OOM"}
  pipeline_replication_lag_seconds{pipeline="cdc-oracle"}
```

### 3.3 Structured Logging

```json
{
  "timestamp": "2025-01-15T10:00:34.567Z",
  "severity": "ERROR",
  "service": "conversion-worker",
  "trace_id": "abc123def456",
  "span_id": "span789",
  "job_id": "job-a1b2c3d4",
  "tenant_id": "tenant-42",
  "message": "AST parsing failed: unsupported CONNECT BY clause",
  "error": {
    "type": "UnsupportedConstructError",
    "construct": "CONNECT_BY_PRIOR",
    "line": 847,
    "file": "proc_get_hierarchy.sql"
  },
  "context": {
    "worker_pod": "worker-pod-abc123",
    "node": "gke-pool-1-abc",
    "memory_usage_mb": 3840,
    "cpu_usage_percent": 78
  }
}
```

**The killer feature: Trace-to-Log correlation.**
Click a trace span in Grafana → see all logs emitted during that span → pinpoint the exact error.

---

## 4. Deep Dive: Tail-Based Sampling

### Problem
At 75K spans/sec, storing all traces costs ~$15K/month. Most successful traces are uninteresting.

### Solution: Tail-Based Sampling in the OTel Collector

```
Head-based sampling:
  Decision made at trace START → can't know if it will error
  Sample 1% → miss 99% of errors

Tail-based sampling:
  Decision made at trace END → knows if trace has errors
  Keep 100% of error traces, 1% of success traces

OTel Collector pipeline:
  receivers:
    otlp:
      protocols:
        grpc: { endpoint: "0.0.0.0:4317" }

  processors:
    tail_sampling:
      decision_wait: 30s
      policies:
        - name: errors-always
          type: status_code
          status_code: { status_codes: [ERROR] }
        - name: high-latency
          type: latency
          latency: { threshold_ms: 5000 }
        - name: probabilistic-default
          type: probabilistic
          probabilistic: { sampling_percentage: 1 }

  exporters:
    otlp/tempo:
      endpoint: "tempo.monitoring:4317"
```

**Cost impact:**
```
Without sampling: 75K spans/sec × 500B = 37.5 MB/sec → $15K/month
With tail sampling: ~1.5K spans/sec → $300/month
Error coverage: 100% (every error trace is captured)
```

---

## 5. Deep Dive: SLO-Based Alerting

### SLO Definitions

```
SLO: 99.5% of conversion jobs complete successfully within 5 minutes

Error budget: 0.5% of 10K jobs/min = 50 allowed failures per minute

Burn rate alert (Google SRE model):
  - 14.4x burn rate over 1 hour → page (critical)
    → Consuming 14.4 hours of error budget per hour
    → Will exhaust monthly budget in ~2 days
  
  - 6x burn rate over 6 hours → ticket (warning)
    → Slower burn, but still unsustainable

PromQL for multi-window burn rate:
  (
    sum(rate(conversion_jobs_total{status="failed"}[1h]))
    / sum(rate(conversion_jobs_total[1h]))
  ) > (14.4 * 0.005)
  AND
  (
    sum(rate(conversion_jobs_total{status="failed"}[5m]))
    / sum(rate(conversion_jobs_total[5m]))
  ) > (14.4 * 0.005)
```

### Alert Severity Matrix

| Condition | Severity | Action |
|-----------|----------|--------|
| Error rate > 5% for 5 min | P1 Critical | PagerDuty page |
| P99 latency > 60s for 10 min | P2 High | PagerDuty page |
| DLQ depth > 500 items | P2 High | Slack + PagerDuty |
| Replication lag > 2 min (CDC) | P1 Critical | PagerDuty page |
| Worker pod restarts > 10/hour | P3 Medium | Slack alert |
| Error budget < 20% remaining | P3 Medium | Weekly review |

---

## 6. Deep Dive: Debugging a Failed Conversion

```
Scenario: Customer reports "my stored procedure converted incorrectly"

Investigation workflow:
  1. Search logs by job_id or filename
     → Find trace_id: abc123def456
  
  2. Open trace in Grafana Tempo
     → See full request flow across 8 services
     → Spot: worker.transpile span has warning attribute
     → Warning: "CONNECT BY clause approximated with recursive CTE"
  
  3. Click span → jump to correlated logs
     → Log: "Unsupported: CONNECT BY NOCYCLE — falling back to recursive CTE without cycle detection"
     → Log: "Output validation: PASS (syntax only, semantic accuracy: 0.85)"
  
  4. Check custom metric: conversion_accuracy_score histogram
     → oracle_plsql average: 0.97
     → This specific file: 0.85 — significant outlier
  
  5. Root cause: Known limitation in CONNECT BY transpilation
     → File routed to manual review queue
     → Alert rule added: conversion_accuracy < 0.90 → flag for human review
```

> *This trace-driven debugging mirrors how I build observability into Praesidium's compliance agents — each accounting validation step produces a span, so when an IRS form fails validation, we trace the exact calculation chain that produced the error.*

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **OTel Collector crash** | Traces/metrics lost | DaemonSet restarts; buffer in agent; alert on collector health |
| **Tempo storage full** | New traces rejected | Retention policy (30 days); compact old traces; alert at 80% |
| **Prometheus scrape failure** | Metric gaps | Redundant Prometheus + Thanos for HA; federated scraping |
| **Alert fatigue** | Engineers ignore alerts | SLO-based burn rate alerts; strict severity matrix |
| **Sampling misses critical trace** | Can't debug failure | Tail-based sampling keeps 100% of errors; force-sample specific tenants |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| OpenTelemetry | Datadog/New Relic | Open-source, vendor-neutral, no per-host licensing |
| Tail-based sampling | Head-based | 100% error capture; head-based misses errors randomly |
| Grafana stack (Tempo/Loki/Prometheus) | Elastic APM | OSS; Grafana correlates all three pillars natively |
| SLO burn-rate alerts | Threshold alerts | Fewer false positives; error-budget-aware; Google SRE best practice |
| DaemonSet OTel Collector | Sidecar per pod | Lower resource overhead; centralized config; easier upgrades |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use Datadog?" | "Datadog is excellent but costs $23/host/month at our scale (100+ pods). The OSS stack (OTel + Grafana + Tempo + Prometheus) gives the same capabilities at a fraction of the cost, and we avoid vendor lock-in on our observability data." |
| "100% error sampling sounds expensive" | "Errors are rare — typically < 5% of traffic. 100% of 5% is only 5% of total trace volume. The real cost savings come from sampling 1% of the 95% success traffic. We get complete error visibility for minimal cost increase." |
| "How do you trace across async boundaries?" | "We embed the W3C `traceparent` header in Pub/Sub message attributes. When a worker picks up a message, it extracts the trace context and creates a child span linked to the original trace. The trace shows the full journey from API call → Pub/Sub → worker → output." |

---

## 10. Summary: Your Interview Narrative

> "I'd build observability on the **three pillars — traces, metrics, logs** — unified by **OpenTelemetry**. Every service instruments with the OTel SDK, propagating trace context via W3C `traceparent` headers (and Pub/Sub attributes for async). The OTel Collector runs as a DaemonSet on GKE with **tail-based sampling** — 100% of error traces, 1% of successes — reducing storage costs by 50x. **RED metrics** (Rate, Errors, Duration) are scraped by Prometheus with Thanos for HA. Structured JSON logs include trace IDs for cross-pillar correlation. Alerting uses **SLO burn rates** instead of static thresholds — we page only when error budget consumption is unsustainable. The debugging workflow: search by job ID → find trace → click through spans → jump to correlated logs → identify root cause."

---

## 11. Key Terms to Drop Naturally

- **OpenTelemetry (OTel)**, **W3C traceparent**
- **Distributed trace**, **span**, **trace context propagation**
- **RED metrics** (Rate, Errors, Duration)
- **Tail-based sampling**, **head-based sampling**
- **SLO**, **error budget**, **burn rate**
- **Grafana**, **Tempo**, **Prometheus**, **Loki**, **Thanos**
- **DaemonSet OTel Collector**
- **Trace-to-log correlation**
