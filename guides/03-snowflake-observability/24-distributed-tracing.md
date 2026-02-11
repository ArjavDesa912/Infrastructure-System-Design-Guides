# Design a Distributed Tracing System

> **Interview Prompt:** "Design a system to trace a request as it flows through dozens of microservices."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How many services in the trace chain? | Complexity and depth |
| 2 | Expected traces per second? | Storage and sampling |
| 3 | How long to retain traces? | Storage design |
| 4 | Do we need real-time trace viewing or batch? | Push vs. batch architecture |
| 5 | Should tracing be automatic or manual instrumentation? | SDK/agent design |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────┐
│  Service Mesh (Instrumented Applications)         │
│                                                    │
│  Service A ──▶ Service B ──▶ Service C            │
│  (Span 1)      (Span 2)      (Span 3)            │
│     │             │             │                  │
│     └─── trace_id: abc-123 ────┘                  │
└──────────┬──────────┬──────────┬─────────────────┘
           │          │          │
           ▼          ▼          ▼
┌──────────────────────────────────────────┐
│         Collector / Agent                 │
│  - Receives spans from all services      │
│  - Batches and forwards                  │
└──────────────────┬───────────────────────┘
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │ Storage  │ │ Indexer  │ │ Analyzer │
   │ (Traces) │ │          │ │ (Deps,   │
   │          │ │          │ │  Anomaly)│
   └──────────┘ └──────────┘ └──────────┘
          │              │
          ▼              ▼
   ┌────────────────────────────┐
   │  Query UI (Jaeger/Zipkin)  │
   └────────────────────────────┘
```

---

## 3. Core Concepts

### 3.1 Trace, Span, and Context

```
Trace: End-to-end journey of a single request
  trace_id: "abc-123"

Span: One unit of work within a trace
  ┌───────────────────────────────────────────────────┐
  │ Trace: abc-123                                     │
  │                                                     │
  │ Span A: API Gateway          [─────────────────]   │
  │ Span B:   Auth Service       [────]                │
  │ Span C:   Job Service              [──────────]    │
  │ Span D:     Database                 [────]        │
  │ Span E:     Queue Publish                  [──]    │
  │                                                     │
  │ Timeline:  0ms   50ms   100ms   150ms   200ms      │
  └───────────────────────────────────────────────────┘

Span data structure:
{
    "trace_id": "abc-123",
    "span_id": "span-C",
    "parent_span_id": "span-A",
    "service": "job-service",
    "operation": "createJob",
    "start_time": "2024-02-10T10:00:00.100Z",
    "duration_ms": 95,
    "status": "OK",
    "tags": {
        "http.method": "POST",
        "http.status": 201,
        "job.type": "conversion"
    },
    "logs": [
        {"time": "...", "message": "Job created: J-42"}
    ]
}
```

### 3.2 Context Propagation

```
How trace_id flows across services:

Service A → Service B (HTTP):
  GET /api/jobs HTTP/1.1
  traceparent: 00-abc123-span-A-01
  
Service B → Service C (gRPC):
  metadata: {"traceparent": "00-abc123-span-B-01"}

Service C → Database (internal):
  Pass trace context via thread-local / async context

W3C Trace Context standard:
  traceparent: {version}-{trace_id}-{parent_span_id}-{flags}
  tracestate: {vendor-specific key-values}
```

---

## 4. Sampling Strategies

At 10K requests/sec, storing every trace is expensive. Sampling is essential.

| Strategy | How It Works | Trade-off |
|----------|-------------|-----------|
| **Head-based sampling** | Decide at the entry point: sample 10% | Simple; may miss interesting traces |
| **Tail-based sampling** | Collect all spans, decide after completion | See errors/slow traces; more expensive |
| **Adaptive sampling** | Increase rate for errors, slow traces | Best of both; complex to implement |
| **Priority sampling** | Always trace certain operations (payments) | Business-critical coverage |

**Recommended: Adaptive sampling**
```
Default: 10% random sampling
Boost to 100% when:
  - Error status (5xx)
  - Latency > p99 threshold
  - Specific user/tenant (debug mode)
  - New deployment (canary monitoring)
```

---

## 5. Storage Design

```
Traces are stored as span collections by trace_id:

Storage options:
  Elasticsearch: Full-text search, good for tag queries
  Cassandra:     High write throughput, good for trace_id lookups
  ClickHouse:    Columnar, great for analytical queries
  
Schema (Cassandra-style):
  Primary key: (trace_id)
  Clustering: span_id
  
  trace_id | span_id | parent | service | operation | duration | tags
  abc-123  | span-A  | null   | gateway | handleReq | 200ms    | {...}
  abc-123  | span-B  | span-A | auth    | validate  | 15ms     | {...}
  abc-123  | span-C  | span-A | job-svc | createJob | 95ms     | {...}

Index on:
  - service name (find all traces for a service)
  - duration (find slow traces)
  - error flag (find failed traces)
  - tags (find traces by custom attributes)
```

---

## 6. Analysis & Visualization

### Service Dependency Map
```
Auto-generated from trace data:

API Gateway ──▶ Auth Service
     │
     ├──▶ Job Service ──▶ Database
     │         │
     │         └──▶ Queue (Kafka)
     │
     └──▶ User Service ──▶ Cache (Redis)
```

### Latency Breakdown
```
Request total: 200ms
  ├── Gateway → Auth:     15ms (7.5%)
  ├── Gateway → Job:      95ms (47.5%)
  │   ├── Job → Database:  40ms (20%)
  │   └── Job → Queue:     10ms (5%)
  ├── Gateway → User:     30ms (15%)
  └── Gateway overhead:   60ms (30%)  ← opportunity for optimization
```

### Anomaly Detection
```
Normal p99 latency for /api/jobs: 150ms
Current p99: 890ms ← ANOMALY

Trace analysis shows:
  - Database span increased from 30ms to 700ms
  - Likely cause: missing index or lock contention
```

---

## 7. Security Considerations

```
Access Control:
  - Trace viewing restricted by tenant/service boundaries
  - RBAC: operators see all, developers see their services
  - Audit log for trace queries (who searched what)

Data Privacy:
  - PII in span tags: redact or hash sensitive values
  - Request/response bodies: opt-in, truncated by default
  - Trace IDs: random UUIDs, no embedded user info

Tamper Resistance:
  - Signed spans: HMAC prevents trace injection attacks
  - Validate traceparent headers (W3C format)
  - Rate limit trace ingestion to prevent abuse
```

---

## 7. Testing Distributed Tracing

```python
# Test: Trace context propagation
def test_trace_propagation():
    # Service A initiates trace
    trace_id = generate_trace_id()
    span_a = create_span("service-a", "operation", trace_id=trace_id)

    # Propagate to service B via HTTP
    headers = propagate_context(span_a)
    assert headers["traceparent"] == f"00-{trace_id}-{span_a.span_id}-01"

    # Service B extracts context
    span_b = extract_context(headers)
    assert span_b.trace_id == trace_id
    assert span_b.parent_id == span_a.span_id

# Test: Adaptive sampling boosts on error
def test_adaptive_sampling():
    sampler = AdaptiveSampler(default_rate=0.1)

    # Normal request
    assert sampler.should_sample(span_attrs={"status": "200"}) == 0.1

    # Error request - should boost to 100%
    assert sampler.should_sample(span_attrs={"status": "500"}) == 1.0

    # Slow request - should boost
    assert sampler.should_sample(span_attrs={"duration_ms": 5000}) == 1.0

# Test: Trace reconstruction
def test_trace_reconstruction():
    spans = [
        {"trace_id": "abc", "span_id": "A", "parent_id": None, "service": "gateway", "duration": 100},
        {"trace_id": "abc", "span_id": "B", "parent_id": "A", "service": "auth", "duration": 20},
        {"trace_id": "abc", "span_id": "C", "parent_id": "A", "service": "job", "duration": 50},
    ]

    trace = reconstruct_trace(spans)

    assert trace.duration == 100  # Root span
    assert len(trace.children) == 2  # auth and job
    assert trace.children[0].service == "auth"
```

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Tracing adds overhead to every request" | "Minimal. The instrumentation adds 1-5μs per span — negligible vs. actual processing time. Network overhead is batched — spans are buffered and sent asynchronously every few seconds. At 10% sampling, 90% of requests have near-zero tracing overhead." |
| "How do you correlate traces with logs and metrics?" | "The three pillars of observability: traces, logs, and metrics are linked by trace_id. Logs include the trace_id field, so you can jump from a slow trace to the exact log entries. Metrics are tagged with span attributes for correlation." |
| "What about tracing across async boundaries?" | "Context propagation through message queues: the producer injects trace context into the message headers. The consumer extracts it and creates a new span linked to the producer's span. This creates a single trace that spans async boundaries (e.g., API → Kafka → Worker)." |
| "How do you debug a specific user's request?" | "We support debug mode: a user or tenant can be flagged for 100% trace sampling for a time window. Combined with trace_id in logs, an engineer can search for all traces for user-123, find the slow one, click into the waterfall view, and see exactly which span is the bottleneck." |
| "What's the difference between tracing and logging?" | "Logs are events within a single service. Traces connect related events across services. A trace says 'request X took 500ms total, 300ms was in DB service.' Logs say 'DB query SELECT * FROM users WHERE id=123 took 300ms.' Together they give the full picture. Link them with trace_id." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **distributed tracing system** using the W3C Trace Context standard for propagation. Each service creates spans with trace_id, span_id, and parent_span_id, propagated via HTTP headers and message metadata. Spans are collected by local agents, batched, and sent to a central collector. I'd use **adaptive sampling** — 10% by default, 100% for errors and slow traces. Storage uses Elasticsearch or ClickHouse for indexed span queries. The UI shows trace waterfalls, service dependency maps, and latency breakdowns. Traces are correlated with logs (via trace_id) and metrics for full observability. Security includes RBAC for trace access, PII redaction from span tags, HMAC signing to prevent trace injection, and rate limiting on trace ingestion."

---

## 10. Key Terms to Drop Naturally

- **Trace**, **span**, **context propagation**
- **W3C Trace Context**, **traceparent**
- **Head-based / tail-based sampling**
- **Three pillars of observability** (traces, logs, metrics)
- **Service dependency graph**
- **OpenTelemetry** (modern standard)
- **Trace waterfall** (visual timeline)
- **Debug mode** (per-user 100% sampling)
- **Baggage** (key-values propagated across services)
