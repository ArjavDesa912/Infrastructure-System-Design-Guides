# Design a System with Separation of Compute & Storage

> **Interview Prompt:** "Design a system where compute and storage scale independently, like Snowflake's architecture."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the workload? (OLTP, OLAP, mixed?) | Read patterns and concurrency |
| 2 | How bursty is demand? (predictable vs. spiky?) | Auto-scaling strategy |
| 3 | Do multiple consumers need the same data simultaneously? | Multi-cluster serving |
| 4 | What's the data size? (GB vs. PB?) | Storage tier design |
| 5 | What latency is acceptable? (ms vs. seconds?) | Cache layer requirements |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Compute Layer                       │
│  (scales independently, stateless, ephemeral)        │
│                                                       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐     │
│  │ Warehouse  │  │ Warehouse  │  │ Warehouse  │     │
│  │    XS      │  │    L       │  │    XL      │     │
│  │ (2 nodes)  │  │ (8 nodes)  │  │ (32 nodes) │     │
│  │            │  │            │  │            │     │
│  │ ┌──────┐  │  │ ┌──────┐  │  │ ┌──────┐  │     │
│  │ │Local │  │  │ │Local │  │  │ │Local │  │     │
│  │ │Cache │  │  │ │Cache │  │  │ │Cache │  │     │
│  │ │(SSD) │  │  │ │(SSD) │  │  │ │(SSD) │  │     │
│  │ └──────┘  │  │ └──────┘  │  │ └──────┘  │     │
│  └────────────┘  └────────────┘  └────────────┘     │
└──────────────────────────┬───────────────────────────┘
                           │ Read from storage
                           ▼
┌──────────────────────────────────────────────────────┐
│                   Storage Layer                       │
│  (scales independently, persistent, shared)          │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │              Cloud Object Store (S3)         │     │
│  │   Data stored as columnar micro-partitions   │     │
│  │   ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐      │     │
│  │   │Part 1│ │Part 2│ │Part 3│ │Part N│      │     │
│  │   │16MB  │ │16MB  │ │16MB  │ │16MB  │      │     │
│  │   └──────┘ └──────┘ └──────┘ └──────┘      │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │           Metadata Service                   │     │
│  │  - Partition map (which data is where)       │     │
│  │  - Statistics (min/max per partition)         │     │
│  │  - Transaction log                           │     │
│  └─────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────┘
```

---

## 3. Why Separate Compute and Storage?

### Traditional Coupled Architecture (Problems)
```
┌─────────────────────┐
│  Server              │
│  CPU ←→ RAM ←→ Disk │  Storage and compute on same machine
│  (8 cores) (64GB) (2TB) │
└─────────────────────┘

Problems:
  - Need more storage? Must buy more compute too (waste)
  - Need more compute? Must buy more storage too (waste)
  - Server dies → data at risk (or expensive replication)
  - Can't scale to zero (always paying for idle compute)
```

### Separated Architecture (Benefits)
```
Compute (virtual warehouses):       Storage (S3):
  - Scale up/down in seconds         - Infinite capacity
  - Pay only when running            - 11 nines durability
  - Spin up multiple clusters        - Pay per GB ($0.023/GB/mo)
  - Scale to zero when idle          - Shared by all compute
```

| Old Model | New Model |
|-----------|-----------|
| Scale compute + storage together | Scale each independently |
| Always paying for both | Pay for compute only when running |
| Data on local disk | Data on shared cloud storage |
| One cluster = one copy | Multiple clusters read same data |
| Fixed capacity | Elastic, auto-scaling |

---

## 4. Deep-Dive: Core Design

### 4.1 Storage Layer: Micro-Partitions

```
Table: "customers" (10 billion rows)

Stored as micro-partitions (16MB compressed each):
  Partition 1: rows 1-50,000
    ├── Column "id":    [1, 2, 3, ..., 50000]
    ├── Column "name":  ["Alice", "Bob", ...]
    ├── Column "region":["US", "US", "EU", ...]
    └── Column "amount":[100.5, 200.3, ...]
  
  Partition 2: rows 50,001-100,000
    └── ...

  Partition N: ...

Metadata per partition:
  - Min/max value per column (enables partition pruning)
  - Row count
  - Byte size
  - Bloom filter for point lookups
```

**Columnar format advantages:**
- Only read columns you query (skip irrelevant ones)
- Better compression (similar data types together)
- Vectorized processing (CPU-friendly)

### 4.2 Compute Layer: Virtual Warehouses

```
Warehouse lifecycle:
  SUSPENDED (no cost) 
      ↓ Query arrives
  PROVISIONING (10-30 seconds, spin up EC2 instances)
      ↓
  RUNNING (processing queries, SSD caching active)
      ↓ Idle timeout (5-15 min)
  SUSPENDING → SUSPENDED (no cost)
```

**Each warehouse:**
- Has its own SSD cache (hot data cached locally)
- Doesn't share compute with other warehouses
- Can scale up (bigger instances) or out (more instances)
- Multiple warehouses can query the same data simultaneously

### 4.3 Local SSD Cache (The Secret Sauce)

```
Query: SELECT * FROM orders WHERE customer_id = 123

1. Compute node checks local SSD cache
   → Cache HIT: read from SSD (microseconds) ✅
   → Cache MISS: fetch from S3 (50-200ms)
              → Cache the data on SSD for future reads

Cache eviction: LRU
Cache scope: Per-warehouse (not shared across warehouses)
Cache invalidation: On data update, mark cached partitions stale
```

**This is what makes the architecture fast despite remote storage.**

---

## 5. Multi-Cluster Serving

```
ETL Pipeline (compute-heavy write):     Analytics Team (read):
┌──────────────────────┐          ┌──────────────────────┐
│  Warehouse: ETL-XL   │          │  Warehouse: BI-L     │
│  32 nodes, 256 cores │          │  8 nodes, 64 cores   │
│  Running 2am-6am     │          │  Running 9am-6pm     │
└──────────┬───────────┘          └──────────┬───────────┘
           │                                  │
           ▼                                  ▼
    ┌──────────────────────────────────────────────┐
    │           Shared Storage (S3)                 │
    │    Same data, no copies, no data movement     │
    └──────────────────────────────────────────────┘
```

**No data copying required** — both warehouses read the same S3 data.

---

## 6. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **S3 read latency (50-200ms)** | Local SSD cache on compute nodes; cache warming |
| **Cold start (warehouse spin-up)** | Pre-warmed pool; keep min 1 node running |
| **Cache misses on new data** | Predictive caching based on query patterns |
| **S3 throttling** | Spread data across S3 prefixes; request rate partitioning |
| **Metadata scanning overhead** | Partition pruning via min/max stats; bloom filters |
| **Concurrent query contention** | Multi-cluster warehouses; auto-scale compute |

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Isn't reading from S3 slow?" | "Yes, first access is 50-200ms. But compute nodes have SSD caches that store recently accessed data. After the first read, subsequent queries hit the cache at microsecond latency. For hot workloads, cache hit rates of 90%+ are typical." |
| "What about consistency?" | "The metadata service is strongly consistent — it acts as the source of truth for table state. When data is written, new partitions are created (immutable), and the metadata atomically points to the new version. Reads always see a consistent snapshot." |
| "Why not just use bigger machines?" | "Vertical scaling has limits — you can't buy a machine with 1PB of RAM. More importantly, separation of compute and storage means you pay for each only when needed. An ETL job that runs 4 hours/day doesn't need to pay for 24 hours of compute and storage." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a system with **separated compute and storage layers**, inspired by Snowflake's architecture. Data is stored in S3 as compressed columnar micro-partitions (16MB each). A metadata service tracks partition locations and column statistics. Compute nodes (virtual warehouses) are stateless and ephemeral — they spin up on demand, cache hot data on local SSDs, and suspend when idle. Multiple warehouses can independently query the same data without contention. This gives us infinite storage scalability, elastic compute, and cost efficiency — you only pay for compute when queries are running."

---

## 9. Key Terms to Drop Naturally

- **Micro-partition**, **columnar storage**
- **Virtual warehouse**, **auto-suspend/auto-resume**
- **SSD cache**, **cache hit ratio**
- **Partition pruning** (min/max statistics)
- **Zero-copy cloning** (metadata-only operation)
- **Elastic compute**, **scale to zero**
