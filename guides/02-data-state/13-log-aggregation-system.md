# Design a Log Aggregation System

> **Interview Prompt:** "Design a system to collect, store, and search error logs from thousands of conversion workers."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Expected log volume? (GB/day, events/sec) | Sizing and architecture choice |
| 2 | What's the search latency requirement? | Real-time indexing vs. batch |
| 3 | How long to retain logs? (7 days, 90 days, years?) | Storage tiering strategy |
| 4 | What structure? (JSON, unstructured text, mix?) | Parsing and indexing approach |
| 5 | Do we need real-time alerting on log patterns? | Streaming vs. batch analysis |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Workers (Log Producers)                                  │
│  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐                │
│  │ W1    │ │ W2    │ │ W3    │ │ W-N   │                │
│  │ Agent │ │ Agent │ │ Agent │ │ Agent │                │
│  └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘                │
└──────┼─────────┼─────────┼─────────┼─────────────────────┘
       │         │         │         │
       ▼         ▼         ▼         ▼
┌──────────────────────────────────────────┐
│          Transport Layer (Kafka)          │
│    Topic: logs-raw (partitioned by        │
│           worker_id for ordering)         │
└──────────────────┬───────────────────────┘
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │  Stream  │ │  Index   │ │  Alert   │
   │ Processor│ │  Writer  │ │  Engine  │
   │ (Enrich) │ │(Elastic) │ │ (Rules)  │
   └────┬─────┘ └────┬─────┘ └────┬─────┘
        │             │             │
        ▼             ▼             ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │  Cold    │ │  Search  │ │  Alert   │
   │  Storage │ │  Index   │ │ Channels │
   │  (S3)   │ │(Elastic) │ │(PagerDuty)│
   └──────────┘ └──────────┘ └──────────┘
```

### Pipeline Stages

1. **Collection** — Lightweight agents on each worker ship logs
2. **Transport** — Kafka buffers and distributes log events
3. **Processing** — Enrich, parse, normalize log entries
4. **Indexing** — Write to Elasticsearch for full-text search
5. **Storage** — Archive to S3 for long-term retention
6. **Alerting** — Pattern-match in real-time for anomalies

---

## 3. Deep-Dive: Core Design

### 3.1 Log Collection Agent

```
Worker Process ──▶ Log Agent (sidecar)
                     │
                     ├── Tail log files (inotify)
                     ├── Buffer in memory (ring buffer, 10MB)
                     ├── Batch + compress (gzip/LZ4)
                     └── Send to Kafka with backpressure

Agent guarantees:
  - At-least-once delivery
  - Tracks file offset (survives agent restart)
  - Backpressure: if Kafka is slow, buffer locally
  - Never blocks the application process
```

Tools: Fluentd, Fluent Bit, Vector, Filebeat

### 3.2 Log Structure

```json
{
    "timestamp": "2024-02-10T10:30:00.123Z",
    "level": "ERROR",
    "service": "conversion-worker",
    "worker_id": "worker-42",
    "trace_id": "abc-123-def",
    "job_id": "job-789",
    "tenant_id": "tenant-456",
    "file": "complex_procedure.sql",
    "message": "Parse error at line 47: unexpected token 'GOTO'",
    "stack_trace": "...",
    "tags": {
        "environment": "production",
        "region": "us-east-1",
        "version": "2.3.1"
    }
}
```

**Key fields for querying:**
- `level`: Filter by severity
- `trace_id`: Follow a request across services
- `job_id` / `tenant_id`: Scope to a specific job or customer
- `timestamp`: Time-range queries

### 3.3 Indexing (Elasticsearch)

```
Index pattern: logs-YYYY-MM-DD (one index per day)

Mapping:
  timestamp  → date    (fast range queries)
  level      → keyword (exact match filter)
  service    → keyword
  message    → text    (full-text search, analyzed)
  trace_id   → keyword
  job_id     → keyword
  tenant_id  → keyword
```

**Why daily indexes?**
- Easy retention: delete old indexes by date
- Performance: smaller indexes are faster to query
- Hot/warm/cold tiering: recent indexes on fast storage, old ones on cheap storage

### 3.4 Storage Tiering

```
Age: 0-7 days     → Hot (Elasticsearch, SSD, full indexing)
Age: 7-30 days    → Warm (Elasticsearch, HDD, reduced replicas)
Age: 30-365 days  → Cold (S3 + Athena for ad-hoc queries)
Age: > 365 days   → Delete (or archive to Glacier for compliance)
```

---

## 4. Search & Query Design

```
Query: "All errors in the last hour for tenant-456"

GET /logs/_search
{
    "query": {
        "bool": {
            "must": [
                {"term": {"level": "ERROR"}},
                {"term": {"tenant_id": "tenant-456"}},
                {"range": {"timestamp": {"gte": "now-1h"}}}
            ]
        }
    },
    "sort": [{"timestamp": "desc"}],
    "size": 100
}
```

**Advanced queries:**
- Full-text search in message: `"message": "parse error"`
- Aggregation: count errors per worker over time
- Pattern detection: "5 errors with same stack trace in 1 minute"

---

## 5. Alerting

```
Alert Rules:
  1. Error rate > 10 errors/min for any service      → WARNING
  2. Error rate > 100 errors/min for any service     → CRITICAL
  3. Specific pattern: "OOM" in message               → CRITICAL
  4. No logs from worker for > 5 minutes              → Dead worker?
  5. New error type (unseen stack trace)               → INVESTIGATION

Alert channels:
  CRITICAL → PagerDuty (page on-call)
  WARNING  → Slack channel
  INFO     → Dashboard only
```

---

## 6. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Log volume spikes** | Kafka absorbs bursts; agents do sampling for verbose logs |
| **Elasticsearch indexing lag** | Increase indexing workers, use bulk API, optimize mapping |
| **Storage costs** | Hot/warm/cold tiering; compress and archive to S3 |
| **Query slowness** | Use filters (keyword) before full-text search; query specific date ranges |
| **Cardinality explosion** | Don't index high-cardinality fields (request IDs in tags) as keywords |

### Capacity Estimation

```
Assumptions:
  1000 workers, each producing 100 log lines/sec
  Average log event: 500 bytes (JSON)
  Retention: hot 7 days, warm 30 days, cold 365 days

Ingestion:
  100K events/sec × 500 bytes = 50 MB/sec = 4.3 TB/day

Storage:
  Hot (Elasticsearch, 2 replicas): 4.3 TB/day × 7 days × 3 = 90 TB
  Warm (Elasticsearch, 1 replica): 4.3 TB/day × 23 days × 2 = 198 TB
  Cold (S3, compressed): 4.3 TB/day × 0.3 × 335 days = 432 TB

Elasticsearch cluster:
  Hot tier: 15 nodes × 6TB SSD each = 90 TB
  Warm tier: 20 nodes × 10TB HDD each = 200 TB

Kafka:
  50 MB/sec × 3 replicas = 150 MB/sec write
  Retention: 24 hours buffer = 4.3 TB × 3 = 13 TB
```

---

## 7. Security Considerations

```
Access Control:
  - RBAC: users can only view logs from their tenant/namespace
  - Audit log search: track who searched what, when
  - Query rate limiting per user (prevent expensive queries)

Data Privacy:
  - PII detection: automatically redact emails, SSNs, API keys
  - Sensitive fields: hash or tokenize before indexing
  - Encrypted transport: TLS for all log shipping
  - Retention policies: auto-delete based on compliance requirements

Log Integrity:
  - Hash chaining: detect tampering with archived logs
  - WORM storage: append-only for compliance (SEC, HIPAA)
  - Signed logs: digital signatures for forensic evidence
```

---

## 7. Testing Log Aggregation

```python
# Test: End-to-end log flow
def test_log_flow():
    # Send log from worker
    send_log("worker-7", level="ERROR", message="Parse failed", file="test.sql")

    # Verify in Kafka
    kafka_msg = consume_kafka("logs-topic", timeout=1)
    assert kafka_msg["worker"] == "worker-7"
    assert kafka_msg["level"] == "ERROR"

    # Wait for indexing
    time.sleep(2)

    # Verify searchable in Elasticsearch
    results = query_elasticsearch({
        "query": {"term": {"file": "test.sql"}},
        "size": 1
    })
    assert len(results) == 1
    assert results[0]["message"] == "Parse failed"

# Test: Log sampling under load
def test_log_sampling():
    sampler = LogSampler(rate=0.1)  # 10% sampling

    sampled_count = 0
    for i in range(1000):
        if sampler.should_log(level="INFO"):
            sampled_count += 1

    # Should be approximately 100 (±20% tolerance)
    assert 80 < sampled_count < 120

    # ERROR logs should never be sampled
    error_count = 0
    for i in range(100):
        if sampler.should_log(level="ERROR"):
            error_count += 1
    assert error_count == 100  # 100% of errors sampled

# Test: Alert triggering
def test_alerting():
    # Send 5 errors in 1 minute
    for i in range(5):
        send_log("worker-1", level="ERROR", message=f"Error {i}")
        time.sleep(0.1)

    # Check alert triggered
    alerts = get_alerts(last_minutes=5)
    assert any("High error rate" in alert["message"] for alert in alerts)
```

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Elasticsearch is expensive at scale" | "We mitigate with tiered storage: only the last 7 days live in hot Elasticsearch. Older logs move to S3, queryable via Athena or ClickHouse for cheaper analytical queries. We also reduce replicas for warm indexes." |
| "How do you handle 100K events/sec?" | "Kafka easily handles this as the buffer. Elasticsearch bulk indexing handles 50K+ docs/sec per node. We shard indexes and scale the indexing pipeline horizontally. Agents compress and batch logs to reduce network overhead." |
| "What about log sampling?" | "For high-volume debug logs, we sample at the agent level (e.g., 10% of debug logs). Error and critical logs are always shipped at 100%. This reduces volume by 80%+ while keeping all actionable data." |
| "How do you handle multi-line logs (stack traces)?" | "The agent uses a multi-line pattern matcher — if a log line starts with whitespace or a known continuation pattern, it's appended to the previous event. This way, a Java stack trace becomes one log event, not 50 separate lines." |
| "What about compliance and audit logging?" | "Audit logs go to a separate, immutable Kafka topic with longer retention. They're written to tamper-proof storage (WORM) in S3. Access to audit logs is restricted by IAM with full audit trail of who queried what." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **log aggregation pipeline with three layers**: Collection (lightweight agents on each worker tail logs and ship via Kafka), Processing (stream processors enrich and normalize logs), and Storage/Search (Elasticsearch for indexed search + S3 for cold archival). Kafka acts as the durable buffer between producers and consumers, absorbing spikes. Logs are indexed into daily Elasticsearch indexes with hot/warm/cold tiering for cost optimization. Real-time alerting watches the stream for error patterns and pages on-call when thresholds breach. The system handles 100K+ events/sec through partitioned Kafka topics, bulk Elasticsearch indexing, and agent-level batching."

---

## 9. Key Terms to Drop Naturally

- **ELK Stack** (Elasticsearch, Logstash, Kibana) or **EFK** (Fluentd)
- **Hot/warm/cold tiering**, **index lifecycle management (ILM)**
- **Full-text search**, **inverted index**
- **Kafka as buffer**, **backpressure**
- **Structured logging**, **trace_id correlation**
- **Log sampling**, **cardinality**
- **Multi-line log aggregation**
- **WORM storage** (Write Once Read Many)
