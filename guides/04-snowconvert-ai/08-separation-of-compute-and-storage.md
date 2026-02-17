# Separation of Compute and Storage

> **Interview Prompt:** "Snowflake's architecture separates compute from storage. Design a Snowflake-style decoupled system for SnowConvert — where conversion workloads scale independently from the stored code, results, and conversion artifacts."

---

## 1. Requirements

### Functional
- Store all migration artifacts (source code, converted code, reports, metadata) in a shared, durable storage layer.
- Spin up ephemeral compute clusters for conversion workloads on demand.
- Multiple compute clusters can read the same data simultaneously without contention.
- Compute scales to zero when idle (no cost for inactive projects).
- Support workload isolation: heavy batch conversion doesn't degrade interactive validation queries.

### Non-Functional
- **Storage durability:** 99.999999999% (11 nines — GCS/S3 standard).
- **Compute start time:** < 30 seconds from request to first file processed.
- **Cost model:** Pay-per-second for compute, pay-per-GB/month for storage.
- **Concurrent readers:** 50+ compute clusters reading same dataset.

### Capacity Estimation
```
Storage:
  Active migrations:      100 tenants × avg 10 GB = 1 TB stored
  Archive (completed):    1000 completed migrations × avg 5 GB = 5 TB
  Monthly storage cost:   6 TB × $0.02/GB = $120/month

Compute:
  Peak concurrent:        200 conversion workers across all tenants
  Compute hours/day:      200 workers × 8 hrs avg = 1,600 pod-hours
  Cost (e2-standard-4):   1,600 × $0.13 = ~$210/day
  
Savings vs coupled:
  Coupled: 200 always-on VMs = $625/day
  Decoupled: $210/day (compute) + $4/day (storage) = 66% savings
```

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Control Plane (Always On)                   │
│                                                               │
│  ┌───────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ API Server│  │ Metadata Svc │  │ Cluster Orchestrator  │ │
│  │ (Envoy)   │  │ (PostgreSQL) │  │ (manages GKE pools)   │ │
│  └─────┬─────┘  └──────┬───────┘  └───────────┬───────────┘ │
└────────│───────────────│──────────────────────│──────────────┘
         │               │                      │
         │               │                      │
┌────────▼───────────────▼──────────────────────▼──────────────┐
│                   Compute Layer (Ephemeral)                    │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Cluster: ETL │  │ Cluster:     │  │ Cluster:     │       │
│  │ (Batch Conv.)│  │ Interactive  │  │ Validation   │       │
│  │ 32 pods      │  │ 4 pods       │  │ 8 pods       │       │
│  │              │  │              │  │              │       │
│  │ ┌──────────┐│  │ ┌──────────┐│  │ ┌──────────┐│       │
│  │ │Local SSD ││  │ │Local SSD ││  │ │Local SSD ││       │
│  │ │  Cache   ││  │ │  Cache   ││  │ │  Cache   ││       │
│  │ └──────────┘│  │ └──────────┘│  │ └──────────┘│       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                │
│    Auto-suspend       Auto-suspend      Auto-suspend        │
│    after 5 min idle   after 2 min idle  after 5 min idle    │
└─────────│─────────────────│─────────────────│────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│                   Storage Layer (Persistent)                  │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  GCS (Object Storage)                  │   │
│  │                                                       │   │
│  │  gs://snowconvert-data/                               │   │
│  │  ├── tenants/{tenant_id}/                             │   │
│  │  │   ├── sources/          (uploaded source SQL)       │   │
│  │  │   ├── converted/        (Snowflake SQL output)      │   │
│  │  │   ├── reports/          (conversion reports)        │   │
│  │  │   └── metadata/         (lineage, configs)          │   │
│  │  └── shared/                                          │   │
│  │      ├── grammar-models/   (AST parser grammars)       │   │
│  │      └── type-mappings/    (dialect conversion rules)  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Deep Dive: Why Decouple?

### The Coupled Problem

```
Traditional: Monolithic conversion server
  ┌─────────────────────────┐
  │  Server (always on)     │
  │  CPU: 8 cores           │  ← Paying for compute 24/7
  │  RAM: 64 GB             │  ← Even when no conversions running
  │  Disk: 2 TB (local SSD) │  ← Data tied to this machine
  │  $625/month             │
  └─────────────────────────┘

Problems:
  1. Need more CPU for big batch? → Buy new server with MORE disk too
  2. Need more storage for archive? → Buy new server with MORE CPU too
  3. Server dies → data at risk (or expensive local RAID)
  4. Two teams need same data? → Copy it (2x storage cost, stale copies)
  5. No work to do? → Still paying full server cost
```

### The Decoupled Solution

```
Compute: "Conversion Clusters"
  - Ephemeral GKE pod groups
  - Spin up in seconds for batch jobs
  - Scale to zero when idle (truly zero cost)
  - Multiple clusters access same data concurrently
  
Storage: GCS
  - Infinite capacity at $0.02/GB/month
  - 11 nines durability (no replication to manage)
  - Shared by all compute clusters (no copies)
  - Data lives forever (survives compute teardown)
```

| Aspect | Coupled | Decoupled |
|--------|---------|-----------|
| Idle cost | $625/month (always on) | $0/month compute + $4/month storage |
| Scale compute | Buy new server (days) | Spin up pods (seconds) |
| Scale storage | Buy new disk (hours) | Automatic (GCS infinite) |
| Data access | Single server only | Multiple clusters concurrently |
| Server failure | Data at risk | Compute is disposable; data in GCS |

> *This architecture directly mirrors Snowflake's virtual warehouses — and maps to how I designed VibeDB's separation between the compute engine (query processing) and the storage engine (LSM tree + SSTables). The query engine can be restarted, scaled, or replaced without affecting stored data.*

---

## 4. Deep Dive: Local SSD Cache

```
Data Access Path:

  Worker needs file: gs://snowconvert-data/tenants/42/sources/proc_orders.sql

  1. Check local SSD cache (NVMe on GKE node)
     → HIT:  read from SSD (0.1ms) ✅
     → MISS: fetch from GCS (50-200ms)
             → Write to local SSD cache
             → Return data to worker

Cache Properties:
  - Scope: Per-cluster (not shared across clusters)
  - Eviction: LRU
  - Size: 100 GB per node (NVMe SSD)
  - Warm-up: First access is slow; subsequent accesses are fast
  - Invalidation: On data mutation → delete cached copy

Typical hit rates:
  Batch conversion: 30-50% (sequential scan, low reuse)
  Interactive validation: 80-95% (same files accessed repeatedly)
  Re-runs: 95%+ (same input, all cached from first run)
```

**Cache Warming Strategy:**
```
When a cluster starts for a known batch:
  1. Read manifest from GCS (list of all files)
  2. Predict hot files: recently touched, frequently accessed
  3. Pre-fetch top 1000 files to local SSD in background
  4. Workers start immediately — cache warms while they work
```

---

## 5. Deep Dive: Multi-Cluster Workload Isolation

```
Scenario: Tenant-42's batch conversion shouldn't slow Tenant-43's interactive queries.

Solution: Separate clusters with independent resources:

  Cluster "batch-tenant-42" (GKE node pool: high-cpu)
  ├── 32 worker pods
  ├── Processing 200K files
  ├── Consuming 128 CPU cores, 256 GB RAM
  └── Reading gs://snowconvert-data/tenants/42/sources/

  Cluster "interactive-tenant-43" (GKE node pool: general)
  ├── 4 worker pods
  ├── Running ad-hoc validation queries
  ├── Consuming 16 CPU cores, 32 GB RAM
  └── Reading gs://snowconvert-data/tenants/43/sources/

  Both read from GCS concurrently.
  GCS handles 5,000+ read req/sec per prefix — no contention.
  Each cluster has its own SSD cache — no cache interference.
```

---

## 6. Deep Dive: Auto-Suspend and Auto-Resume

```
Cluster Lifecycle:

  SUSPENDED (zero cost, no pods running)
       │
       ▼  API request arrives for this tenant
  PROVISIONING (15-30 seconds)
       │  GKE scales node pool → pods start → SSD cache cold
       ▼
  RUNNING (processing conversion jobs)
       │  Workers pull from Pub/Sub, read from GCS, write results
       │
       │  Idle detection: no jobs processed for 5 minutes
       ▼
  COOLING_DOWN (30-second grace period)
       │  If new job arrives → back to RUNNING
       │  Otherwise →
       ▼
  SUSPENDED (pods terminated, GKE scales node pool to 0)

Cost savings:
  Tenant with 4-hour daily batch job:
    Coupled:   24 hours × $0.13/hr = $3.12/day
    Decoupled: 4 hours × $0.13/hr  = $0.52/day  (83% savings)
```

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **GCS outage** | All clusters can't read/write | Multi-region GCS bucket; retry with backoff; buffer writes locally |
| **Cold cache (slow first access)** | High latency on cluster start | Cache warming; accept cold-start penalty (only 15-30s) |
| **GCS rate limiting** | Slow reads under heavy load | Spread objects across GCS prefixes; request rate partitioning |
| **Cluster fails mid-job** | Work lost since last checkpoint | Checkpoint system (guide 06); idempotent workers resume |
| **Metadata service down** | Can't locate data | PostgreSQL HA (Cloud SQL); cache metadata in local SSD |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| GCS over local disk | NFS / Persistent Disk (PD) | GCS: infinite, durable, $0.02/GB; PD: limited, more expensive |
| SSD cache per cluster | Shared Redis cache | Per-cluster avoids noisy neighbor; simpler; no cache coordination |
| Auto-suspend after 5 min | Always-on clusters | 83% cost reduction for bursty workloads |
| Separate node pools per workload | Single shared pool | Resource isolation; batch jobs can't evict interactive pods |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Isn't GCS read latency a problem?" | "First access is 50-200ms, but NVMe SSD caching on compute nodes brings subsequent reads to 0.1ms. For batch conversion (sequential scan, each file read once), the cold-read cost is amortized over the processing time. For interactive workflows, cache hit rates reach 90%+ since users re-access the same files." |
| "Why not just scale the same cluster?" | "A single shared cluster creates noisy-neighbor problems — a 200K-file batch job consumes all CPU, starving interactive queries. Separate clusters with independent resource pools guarantee isolation. This is exactly how Snowflake's multi-cluster warehouses work." |
| "What about data consistency across clusters?" | "GCS provides strong read-after-write consistency. When Cluster A writes a converted file, Cluster B immediately sees it. There's no stale-read risk. The metadata service in PostgreSQL with serializable isolation provides the coordination layer for job state." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **Snowflake-style decoupled architecture** with three layers: a persistent **GCS storage layer** for all migration artifacts, ephemeral **GKE compute clusters** that spin up on demand and scale to zero when idle, and a lightweight **control plane** (API server + metadata service + cluster orchestrator) that coordinates them. Compute clusters use local NVMe SSD caching to mitigate GCS read latency — cache hit rates reach 90%+ for interactive workflows. Multiple clusters can read the same GCS data concurrently without contention. Auto-suspend after 5 minutes of idle time reduces compute costs by 83% for bursty workloads. This separation means we scale compute and storage independently — a 10TB migration archive costs $200/month in storage, and compute costs $0 when no jobs are running."

---

## 11. Key Terms to Drop Naturally

- **Separation of compute and storage**, **decoupled architecture**
- **Ephemeral compute**, **stateless workers**
- **Auto-suspend**, **auto-resume**, **scale to zero**
- **Local SSD cache**, **cache warming**, **LRU eviction**
- **Multi-cluster isolation**, **workload isolation**
- **GCS strong consistency**, **read-after-write**
- **Virtual warehouse** (Snowflake analogy)
- **Object storage**, **micro-partitions**
