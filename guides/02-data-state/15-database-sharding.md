# Design a Database Sharding Scheme

> **Interview Prompt:** "How do you split customer metadata across multiple databases as you scale?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the data volume and growth rate? | When sharding becomes necessary |
| 2 | What are the primary access patterns? (by user, by tenant, by time?) | Determines the shard key |
| 3 | Do we need cross-shard queries? (joins, aggregations?) | Complexity of query routing |
| 4 | What's the acceptable downtime for resharding? | Online vs. offline migration |
| 5 | Is the data distribution uniform or skewed? | Hot shard prevention |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌─────────────────────┐
│  App      │────▶│  Query Router /     │
│  Server   │     │  Shard Proxy        │
└──────────┘     └─────────┬───────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Shard 0 │ │ Shard 1 │ │ Shard 2 │
         │Tenants  │ │Tenants  │ │Tenants  │
         │ A,D,G   │ │ B,E,H   │ │ C,F,I   │
         └────┬────┘ └────┬────┘ └────┬────┘
              │           │           │
         ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
         │ Replica │ │ Replica │ │ Replica │
         └─────────┘ └─────────┘ └─────────┘
```

---

## 3. Sharding Strategies

### 3.1 Hash-Based Sharding

```
shard_id = hash(tenant_id) % num_shards

Example (4 shards):
  tenant_A → hash("A") % 4 = 2 → Shard 2
  tenant_B → hash("B") % 4 = 0 → Shard 0
  tenant_C → hash("C") % 4 = 1 → Shard 1
```

- ✅ Even distribution (if hash is uniform)
- ❌ Resharding (changing shard count) requires moving data
- ❌ No range queries across shards

### 3.2 Range-Based Sharding

```
Shard 0: tenant_id A-H
Shard 1: tenant_id I-P
Shard 2: tenant_id Q-Z

Or by time:
Shard 0: data from 2024-01 to 2024-03
Shard 1: data from 2024-04 to 2024-06
```

- ✅ Range queries within a shard are efficient
- ❌ Uneven distribution (some ranges are hotter)
- ❌ Growing tail shard gets all new data

### 3.3 Directory-Based Sharding (Lookup Table)

```
Shard Map (in config DB):
  tenant_A → Shard 2
  tenant_B → Shard 0
  tenant_C → Shard 1
  tenant_D → Shard 2
```

- ✅ Full control over placement, easy to rebalance
- ❌ Lookup adds latency (cache the map)
- ❌ Shard map is a dependency

### 3.4 Tenant-Based Sharding (Recommended for SaaS)

```
Small tenants:  Packed together (multi-tenant shards)
  Shard 0: [tenant_1, tenant_2, ..., tenant_100]

Large tenants:  Dedicated shard (single-tenant)
  Shard 5: [tenant_enterprise_A]  ← exclusive
```

- ✅ Isolation for large tenants, efficient packing for small ones
- ✅ Large tenant can have dedicated resources
- ❌ More complex placement logic

### Strategy Comparison Matrix

| Criteria | Hash | Range | Directory | Tenant-Based |
|----------|------|-------|-----------|-------------|
| **Even distribution** | ✅ Great | ❌ Skewed | ✅ Controllable | ✅ With placement |
| **Range queries** | ❌ No | ✅ Yes | ❌ No | ❌ No |
| **Resharding ease** | ❌ Hard | Medium | ✅ Easy | ✅ Easy |
| **Hotspot handling** | Medium | ❌ Poor | ✅ Manual | ✅ Dedicate shard |
| **Best for** | Uniform data | Time-series | SaaS with control | Multi-tier SaaS |

---

## 4. The Shard Key Decision

**The most critical decision in sharding.** A bad shard key = hot shards and cross-shard queries.

| Candidate Key | Pros | Cons |
|--------------|------|------|
| `tenant_id` | All tenant data on one shard, no cross-shard joins | Large tenant = hot shard |
| `user_id` | Even distribution | Cross-shard for tenant-level queries |
| `created_at` | Great for time-series | Hot shard (latest data) |
| `composite: tenant_id + resource_id` | Even within tenant | Complex routing |

**Rule of thumb:** Shard by the most common query filter. For SaaS, that's usually `tenant_id`.

---

## 5. Cross-Shard Operations

The hardest problem in sharding.

### Cross-Shard Query (Scatter-Gather)
```
Query: "Count all jobs across all tenants"

Router:
  → Query Shard 0: COUNT(*)  → 5,230
  → Query Shard 1: COUNT(*)  → 4,890
  → Query Shard 2: COUNT(*)  → 5,100
  
  Aggregate: 5,230 + 4,890 + 5,100 = 15,220
```

### Cross-Shard Transaction (2PC)
```
Transfer credits from Tenant A (Shard 1) to Tenant B (Shard 2):

  Phase 1 (Prepare):
    Shard 1: PREPARE (lock A's balance)    → OK
    Shard 2: PREPARE (lock B's balance)    → OK

  Phase 2 (Commit):
    Shard 1: COMMIT (debit A)
    Shard 2: COMMIT (credit B)
```

- 2PC is slow and complex — **avoid if possible**
- Alternative: **Saga pattern** (compensating transactions)

---

## 6. Resharding (Adding/Removing Shards)

### Online Resharding (Zero-Downtime)

```
Phase 1: Double-Write
  New writes go to BOTH old shard and new shard

Phase 2: Backfill  
  Copy existing data from old shard to new shard

Phase 3: Cutover
  Switch reads to new shard
  Stop writes to old shard

Phase 4: Cleanup
  Verify data consistency
  Delete old copies
```

### Consistent Hashing for Dynamic Sharding

```
Ring: [0 ─────── Shard A ────── Shard B ────── Shard C ─── 2^32]

Add Shard D:
  Only data between C and D moves
  Other shards unaffected
```

### Virtual Shards (Recommended)

```
Instead of physical shards, use virtual shards (many more than physical):

  256 virtual shards mapped to 4 physical databases:
  
  Virtual 0-63   → Physical DB-1
  Virtual 64-127  → Physical DB-2
  Virtual 128-191 → Physical DB-3
  Virtual 192-255 → Physical DB-4

Shard routing:
  shard = hash(tenant_id) % 256    → virtual shard 142
  physical_db = shard_map[142]      → DB-3

To add a new physical DB:
  Move virtual shards 192-255 → DB-4 and DB-5 (split)
  No hash function change needed
  Only move the virtual shards, not rehash everything
```

---

## 7. Capacity Estimation

```
Assumptions:
  1,000 tenants, 100M total rows
  Average 100K rows per tenant (skewed: top 10 tenants have 10M each)
  Row size: 1KB average → 100GB total data
  Write throughput: 5K writes/sec
  Read throughput: 50K reads/sec

Sharding plan:
  4 shards initially, each handling:
    25GB data, 1.25K writes/sec, 12.5K reads/sec
  
  Top 10 tenants (10M rows each = 10GB each):
    Move to dedicated shards (10 additional shards)

Growth plan:
  When any shard exceeds 50GB or 3K writes/sec → split
  256 virtual shards gives headroom for 64 physical DBs
```

---

## 8. Monitoring & Alerts

```
Per-shard metrics to track:
  - Disk usage (% full) → alert at 75%
  - QPS (reads + writes) → alert at 80% capacity
  - Replication lag (if using replicas) → alert at >5 seconds
  - Query latency (p50, p95, p99) → alert if p99 > 500ms
  - Connection pool utilization → alert at 80%

Cross-shard metrics:
  - Shard size skew (max/min ratio) → alert if > 3x
  - Cross-shard query rate → investigate if > 5% of total queries
  - Failed cross-shard transactions → alert on any
```

---

## 9. Security Considerations

```
Tenant Isolation:
  - Database-level isolation: separate DBs per tenant for sensitive customers
  - Row-level security: tenant_id filtering enforced at DB level
  - Connection pooling: separate pools per tenant tier (free vs. enterprise)

Data Protection:
  - Encryption at rest: per-shard encryption keys
  - Encryption in transit: TLS for all connections
  - Backup encryption: encrypted backups with key rotation

Access Control:
  - Per-tenant roles: admin, editor, viewer
  - Audit logging: track cross-tenant access attempts
  - Network isolation: VPC per shard tier
```

---

## 9. Testing Database Sharding

```python
# Test: Shard routing correctness
def test_shard_routing():
    # Shard by tenant_id % 4
    num_shards = 4

    for tenant_id in range(100):
        expected_shard = tenant_id % num_shards
        actual_shard = route_to_shard(tenant_id)
        assert actual_shard == f"shard-{expected_shard}"

# Test: Cross-shard query detection
def test_cross_shard_detection():
    # Single-shard query (should succeed)
    result = execute_query(
        "SELECT * FROM orders WHERE tenant_id = 42",
        tenant_id=42
    )
    assert result.shard_count == 1

    # Cross-shard query (should warn/fail)
    with pytest.raises(CrossShardQueryError):
        execute_query(
            "SELECT * FROM orders WHERE amount > 1000",  # No tenant filter
            tenant_id=42
        )

# Test: Rebalancing without downtime
def test_rebalancing():
    # Initial state: 2 shards
    assert count_virtual_shards_on_physical("shard-0") == 128
    assert count_virtual_shards_on_physical("shard-1") == 128

    # Rebalance virtual shard 0-63 from shard-0 to shard-2
    rebalance(virtual_range=(0, 63), from_shard="shard-0", to_shard="shard-2")

    # Verify new distribution
    assert count_virtual_shards_on_physical("shard-0") == 64
    assert count_virtual_shards_on_physical("shard-1") == 128
    assert count_virtual_shards_on_physical("shard-2") == 64

    # Verify data integrity (reads still work)
    for tenant_id in range(64):
        data = read_from_shard(tenant_id=tenant_id)
        assert data is not None
```

---

## 10. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "When should you shard vs. just scaling vertically?" | "I'd shard when vertical scaling hits its limit — typically when a single database can't handle the write throughput or the data no longer fits on one machine. For most systems, that's at 1-10TB of data or 10K+ writes/sec. Before sharding, I'd try read replicas and caching first." |
| "How do you handle a hot shard?" | "If one tenant dominates a shard, I'd move them to a dedicated shard. For hash-based hotspots, I'd add virtual shards or use consistent hashing to rebalance. Monitoring shard utilization metrics (CPU, disk, queries) catches hotspots early." |
| "What about cross-shard joins?" | "I avoid them by co-locating related data on the same shard (shard by tenant_id, so all tenant data is together). For truly cross-shard analytics, I'd use a separate OLAP system (like Snowflake or ClickHouse) that aggregates data from all shards." |
| "How do you migrate from unsharded to sharded?" | "Start with virtual shards (256) on a single DB. This means your routing logic is already in place. When you need physical sharding, move virtual shard ranges to new DBs — no application code changes needed." |
| "What happens during resharding?" | "Online resharding: double-write to both old and new, backfill missing data, cutover reads, then stop old writes. Virtual shards make this easier — you're just reassigning virtual-to-physical mappings." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **tenant-based sharding scheme** with virtual shards for customer metadata. The system starts with 256 virtual shards mapped to physical databases. Each tenant's data lives entirely on one shard, eliminating cross-shard joins for tenant-scoped queries. The shard map is stored in a configuration database and cached at the query router. Small tenants share shards; large enterprise tenants get dedicated shards for isolation and performance. When a shard gets hot, I rebalance by reassigning virtual-to-physical mappings. For cross-tenant analytics, data is replicated to a separate OLAP store. Reharding is done online with a double-write strategy to avoid downtime."

---

## 11. Key Terms to Drop Naturally

- **Shard key**, **partition key**
- **Consistent hashing**, **virtual nodes / virtual shards**
- **Scatter-gather** (cross-shard queries)
- **Two-Phase Commit (2PC)**, **Saga pattern**
- **Resharding**, **rebalancing**, **split-brain**
- **Co-location** (keeping related data together)
- **Double-write migration** (online resharding)
- **Shard proxy / query router**
