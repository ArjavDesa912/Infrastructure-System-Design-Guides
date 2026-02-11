# Design a Metric Monitoring System

> **Interview Prompt:** "Design a system like Prometheus or Datadog for collecting and querying metrics."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Push-based or pull-based collection? | Architecture decision |
| 2 | What's the metric volume? (10K or 10M series?) | Storage and query engine |
| 3 | What's the retention? (30 days, 1 year?) | Downsampling and storage tiering |
| 4 | What query latency is acceptable? | In-memory vs. disk lookups |
| 5 | Do we need alerting built-in? | Alert engine integration |

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────┐
│  Applications & Infrastructure                    │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐           │
│  │App A │ │App B │ │DB    │ │K8s   │           │
│  │/metrx│ │/metrx│ │node  │ │nodes │           │
│  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘           │
└─────┼────────┼────────┼────────┼────────────────┘
      │        │        │        │
      ▼        ▼        ▼        ▼
┌──────────────────────────────────────────┐
│         Collection Layer                  │
│  Pull (Prometheus) or Push (StatsD)      │
│  Scrape targets every 15-60 seconds      │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│          Storage Engine (TSDB)            │
│  ┌──────┐  ┌──────┐  ┌──────┐           │
│  │Block │  │Block │  │Block │           │
│  │0-2h  │  │2-4h  │  │4-6h  │           │
│  └──────┘  └──────┘  └──────┘           │
│  Time-series optimized storage            │
└──────────────────┬───────────────────────┘
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │ Query    │ │ Alert    │ │ Dashboard│
  │ Engine   │ │ Manager  │ │ (Grafana)│
  └──────────┘ └──────────┘ └──────────┘
```

---

## 3. Deep-Dive: Core Design

### 3.1 Data Model

```
Metric: time-series of (timestamp, value) pairs with labels

Example:
  http_requests_total{method="GET", endpoint="/api/jobs", status="200"}

  Timestamp          │ Value
  ────────────────────┼──────
  2024-02-10T10:00:00 │ 1523
  2024-02-10T10:00:15 │ 1547
  2024-02-10T10:00:30 │ 1568
  2024-02-10T10:00:45 │ 1592

Metric types:
  Counter:   monotonically increasing (total requests)
  Gauge:     can go up/down (CPU usage, queue depth)
  Histogram: distribution of values (request latency buckets)
  Summary:   pre-computed quantiles (p50, p95, p99)
```

### 3.2 Collection: Pull vs. Push

| Feature | Pull (Prometheus) | Push (StatsD/DataDog) |
|---------|-------------------|----------------------|
| How | Server scrapes target `/metrics` | Target sends to collector |
| Discovery | Service discovery needed | Target knows the collector |
| Control | Server controls rate | Target controls rate |
| Short-lived jobs | Misses data (not running when scraped) | Works (pushes data) |
| Network | Server initiates (easier firewall) | Target initiates |
| Use case | Infrastructure monitoring | App-level metrics, lambda |

### 3.3 Time-Series Database (TSDB) Storage

```
On-disk format (Prometheus-style):

data/
  01-block-2h/          ← 2-hour time blocks
    meta.json           ← block metadata
    chunks/
      000001            ← compressed time-series chunks
      000002
    index               ← inverted index (label → series)
    tombstones          ← deleted series markers
  02-block-2h/
    ...
  
WAL (Write-Ahead Log):
  wal/
    00000001            ← recent data (not yet compacted)
    00000002
```

**Compaction:**
```
Head (in-memory, last 2h)
  ↓ flush
Block 1 (2h)  Block 2 (2h)  Block 3 (2h)
  ↓              ↓              ↓
  └──── merge ────┘──── merge ──┘
           ↓
     Block merged (6h, smaller, deduplicated)
```

### 3.4 Compression

```
Delta-of-delta encoding for timestamps:
  Raw:    [1707500400, 1707500415, 1707500430, 1707500445]
  Delta:  [-, 15, 15, 15]
  DoD:    [-, -, 0, 0]   ← mostly zeros (highly compressible)

Gorilla compression for values:
  XOR consecutive values, most bits are zero
  Store only the differing bits

Result: 16 bytes of raw data → 1-2 bits per sample
  1 billion samples = ~250MB (vs ~16GB raw)
```

---

## 4. Query Language (PromQL-style)

```
# Current rate of HTTP requests
rate(http_requests_total{method="GET"}[5m])

# CPU usage above 80%
cpu_usage{instance=~"worker-.*"} > 0.8

# 99th percentile request latency
histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))

# Top 5 busiest endpoints
topk(5, rate(http_requests_total[1h]))

# Ratio of errors to total
sum(rate(http_requests_total{status=~"5.."}[5m]))
/
sum(rate(http_requests_total[5m]))
```

---

## 5. Alerting

```
Alert Rule:
  name: HighErrorRate
  expr: sum(rate(http_requests_total{status=~"5.."}[5m])) 
        / sum(rate(http_requests_total[5m])) > 0.05
  for: 5m      ← must be true for 5 minutes
  labels:
    severity: critical
  annotations:
    summary: "Error rate above 5% for 5 minutes"

Alert Flow:
  Rule evaluated every 15s
    → Pending (first trigger, start timer)
    → Firing (sustained for 5m)
    → Alert Manager
      → Dedup, group, route
      → PagerDuty / Slack / Email
```

---

## 6. Scaling: Federation & Remote Storage

```
Local Prometheus (per datacenter/cluster):
  ┌────────┐  ┌────────┐  ┌────────┐
  │ Prom   │  │ Prom   │  │ Prom   │
  │ DC-1   │  │ DC-2   │  │ DC-3   │
  └────┬───┘  └────┬───┘  └────┬───┘
       │           │           │
       ▼           ▼           ▼
  ┌────────────────────────────────┐
  │  Global View / Long-term       │
  │  (Thanos / Cortex / Mimir)    │
  │  - Dedup across replicas       │
  │  - Query across all clusters   │
  │  - Long-term storage (S3)      │
  └────────────────────────────────┘
```

---

## 7. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **High cardinality labels** | Limit label values; drop high-cardinality labels (user IDs) |
| **Query timeout (large range)** | Recording rules (pre-computed aggregations), query limits |
| **Scrape lag** | Increase scrape workers, reduce scrape interval selectively |
| **Storage growth** | Downsampling (5m→1h→1d resolution), tiered storage (local→S3) |
| **Alert storms** | Alert grouping, inhibition rules, cooldown periods |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "How do you handle millions of time series?" | "The TSDB uses an inverted index mapping labels to series, similar to Elasticsearch. Block-based storage with compaction keeps query performance high. High-cardinality mitigation: limit unique label values and use recording rules for pre-aggregation." |
| "Pull-based scraping misses short-lived processes" | "For short-lived jobs, we use a push gateway — the job pushes its metrics to a gateway, and Prometheus scrapes the gateway. This is the standard pattern for batch jobs and lambdas." |
| "How do you query across data centers?" | "Federation. Each DC runs a local Prometheus. A global query layer (like Thanos) can fan out queries across all instances, dedup overlapping data, and merge results. Long-term data is archived to S3 for cost efficiency." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **pull-based metric monitoring system** inspired by Prometheus. Targets expose a `/metrics` endpoint; the collector scrapes them every 15 seconds. Metrics are stored in a TSDB optimized for time-series data — delta-of-delta encoding for timestamps and Gorilla compression for values achieve 16:1 compression. The query engine supports PromQL-style queries for aggregation, rate calculation, and quantiles. Alerting evaluates rules continuously, with pending→firing state transitions and alert grouping to prevent storms. For multi-cluster scaling, I'd use a federation layer (Thanos/Cortex) with long-term S3 storage."

---

## 10. Key Terms to Drop Naturally

- **TSDB** (Time-Series Database), **blocks**, **compaction**
- **Pull-based scraping**, **push gateway**
- **Counter, Gauge, Histogram, Summary** (metric types)
- **PromQL**, **recording rules**
- **Delta-of-delta encoding**, **Gorilla compression**
- **High cardinality** (the #1 performance problem)
- **Federation**, **Thanos/Cortex**
