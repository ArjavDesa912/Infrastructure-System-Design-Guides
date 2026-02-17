# Distributed Checkpointing System

> **Interview Prompt:** "A terabyte-scale data migration is 18 hours into a 20-hour run when a GKE node crashes. Design a checkpointing system that lets the migration resume from where it left off — not restart from scratch."

---

## 1. Requirements

### Functional
- Periodically persist migration progress so work can resume after any failure.
- Support both file-level and row-level checkpoint granularity (files for code migration, rows for data migration).
- Guarantee exactly-once semantics: no data is duplicated or skipped on resume.
- Support concurrent checkpoint writes from multiple workers without coordination overhead.
- Provide checkpoint inspection/management API for operators.

### Non-Functional
- **Checkpoint frequency:** Every 30 seconds or every 1,000 records processed, whichever comes first.
- **Checkpoint write latency:** < 100ms P99.
- **Recovery time:** Resume within 60 seconds of a crash.
- **Storage overhead:** < 1% of total migration data size.

### Capacity Estimation
```
Migration size:       1 TB, 500 tables, 2B total rows
Migration duration:   ~20 hours
Workers:              50 concurrent workers
Checkpoint frequency: Every 30s per worker → 50 × 2/min = 100 checkpoints/min
Checkpoint record:    ~2 KB (table, offset, watermark, metadata)
Total checkpoints:    100/min × 1,200 min = 120K checkpoints
Storage:              120K × 2 KB = ~240 MB (negligible)
```

---

## 2. API Design

### Create Checkpoint
```
POST /v1/checkpoints
{
  "migration_id": "mig-7f3a",
  "worker_id": "worker-pod-abc123",
  "table": "ORDERS.ORDER_ITEMS",
  "checkpoint_type": "row_offset",
  "position": {
    "offset": 45000000,
    "watermark": "2025-01-15T10:30:00Z",
    "partition_key": "order_date",
    "partition_value": "2024-06"
  },
  "stats": {
    "rows_processed": 45000000,
    "bytes_processed": 22500000000,
    "errors": 12
  }
}
```

### Get Latest Checkpoints (for recovery)
```
GET /v1/migrations/mig-7f3a/checkpoints/latest

{
  "migration_id": "mig-7f3a",
  "checkpoints": [
    {
      "table": "ORDERS.ORDER_ITEMS",
      "last_position": {"offset": 45000000, "partition_value": "2024-06"},
      "last_checkpoint_at": "2025-01-15T10:30:00Z",
      "percent_complete": 72.3
    },
    {
      "table": "HR.EMPLOYEES",
      "last_position": {"offset": 1000000},
      "last_checkpoint_at": "2025-01-15T10:30:02Z",
      "percent_complete": 100.0
    }
  ],
  "overall_percent_complete": 68.5
}
```

---

## 3. High-Level Architecture

```
┌───────────────────────────────────────────────────────────┐
│                    Worker Fleet (GKE)                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker N │     │
│  │ Table A │  │ Table B │  │ Table A │  │ Table C │     │
│  │ Part 1  │  │ Full    │  │ Part 2  │  │ Full    │     │
│  │         │  │         │  │         │  │         │     │
│  │ Every 30s or 1K rows:                         │      │
│  │ → Write checkpoint to Checkpoint Store        │      │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘     │
│       │            │            │            │           │
└───────│────────────│────────────│────────────│───────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌──────────────────────────────────────────────────────────┐
│              Checkpoint Store (PostgreSQL)                 │
│                                                           │
│  Table: checkpoints                                       │
│  ┌─────────────┬────────────┬─────────┬────────────────┐ │
│  │migration_id │ table_name │ worker  │ position       │ │
│  ├─────────────┼────────────┼─────────┼────────────────┤ │
│  │ mig-7f3a    │ ORDERS     │ wrk-1   │ offset=45M     │ │
│  │ mig-7f3a    │ EMPLOYEES  │ wrk-2   │ offset=1M(done)│ │
│  │ mig-7f3a    │ ORDERS     │ wrk-3   │ offset=38M     │ │
│  └─────────────┴────────────┴─────────┴────────────────┘ │
│                                                           │
│  On recovery:                                             │
│  1. SELECT * FROM checkpoints WHERE migration_id='mig-7f3a'│
│  2. For each table: resume from MAX(offset)               │
│  3. Relaunch workers with resume positions                │
└──────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive: Checkpoint Strategies

### 4.1 Row-Offset Checkpointing (Data Migration)

```
Table: ORDERS.ORDER_ITEMS (1.2 billion rows)
Partitioned by: order_date (monthly partitions)

Worker processes partition "2024-06":
  Row 0          → Row 1,000    → Checkpoint: offset=1000
  Row 1,001      → Row 2,000    → Checkpoint: offset=2000
  ...
  Row 44,999,001 → Row 45,000,000 → Checkpoint: offset=45M
  ← CRASH HERE →
  
Recovery:
  1. Read checkpoint: last offset = 45,000,000
  2. Resume query: SELECT * FROM ORDERS WHERE order_date = '2024-06'
                   ORDER BY rowid OFFSET 45000000
  3. Continue from row 45,000,001

BUT: OFFSET-based resume is SLOW on large tables (full scan to skip)
```

**Better: Watermark-based checkpointing:**

```
Instead of OFFSET, use a monotonically increasing column as watermark:

  Worker processes: SELECT * FROM ORDERS 
                    WHERE order_date = '2024-06'
                    ORDER BY order_id

  Checkpoint: watermark = last processed order_id = 987654321

  Recovery: SELECT * FROM ORDERS 
            WHERE order_date = '2024-06' 
              AND order_id > 987654321
            ORDER BY order_id

  → Uses index on order_id → instant seek, no full scan
```

> *This watermark-based checkpoint is directly analogous to how VibeDB's compaction process tracks progress — the compaction cursor stores the last key processed, and on restart, it seeks to that key in the SSTable using the sparse index rather than scanning from the beginning.*

### 4.2 File-Level Checkpointing (Code Migration)

```
200,000 SQL files to convert:

  Manifest with status:
  ┌──────────────────┬───────────┐
  │ file_id          │ status    │
  ├──────────────────┼───────────┤
  │ f-0001           │ COMPLETED │
  │ f-0002           │ COMPLETED │
  │ ...              │ COMPLETED │
  │ f-142857         │ COMPLETED │
  │ f-142858         │ RUNNING   │ ← Worker was here when crash happened
  │ f-142859         │ PENDING   │
  │ ...              │ PENDING   │
  └──────────────────┴───────────┘

  Recovery:
  1. Load manifest from GCS
  2. Find all files with status != COMPLETED
  3. Re-publish those file IDs to Pub/Sub
  4. Workers pick up from where fleet left off
```

### 4.3 Checkpoint Consistency: Write-Ahead Pattern

```
Correct ordering for exactly-once:

  1. Process data batch (rows 45001-46000)
  2. Write output to Snowflake (COPY INTO / MERGE)
  3. Wait for Snowflake COMMIT confirmation ← must succeed
  4. Write checkpoint (offset=46000)         ← only after commit
  
  If crash between step 2 and 4:
  → Checkpoint still reads 45000
  → On recovery: re-process rows 45001-46000
  → MERGE INTO is idempotent → no duplicates

  If crash between step 3 and 4:
  → Same recovery — MERGE handles the re-application safely

  NEVER: Write checkpoint BEFORE data commit
  → That would cause data loss (checkpoint says "done" but data isn't there)
```

---

## 5. Deep Dive: Multi-Worker Coordination

### 5.1 Table Partitioning for Parallelism

```
Table ORDERS (1.2B rows) split across 4 workers:

  Worker 1: WHERE order_date BETWEEN '2024-01' AND '2024-03'
  Worker 2: WHERE order_date BETWEEN '2024-04' AND '2024-06'
  Worker 3: WHERE order_date BETWEEN '2024-07' AND '2024-09'
  Worker 4: WHERE order_date BETWEEN '2024-10' AND '2024-12'

Each worker maintains its own checkpoint independently.
No coordination needed — partitions are non-overlapping.

Checkpoint store:
  (mig-7f3a, ORDERS, worker-1, partition=2024-Q1, offset=75M)
  (mig-7f3a, ORDERS, worker-2, partition=2024-Q2, offset=45M)
  (mig-7f3a, ORDERS, worker-3, partition=2024-Q3, offset=80M)
  (mig-7f3a, ORDERS, worker-4, partition=2024-Q4, offset=60M)
```

### 5.2 Worker Failure & Reassignment

```
Worker 3 crashes:
  1. GKE detects pod failure (liveness probe fails)
  2. Checkpoint for worker-3 shows: partition=2024-Q3, offset=80M
  3. Orchestrator reassigns partition 2024-Q3 to a new Worker 5
  4. Worker 5 reads checkpoint and resumes from offset 80M
  5. Remaining workers (1, 2, 4) are unaffected

Key: Workers are STATELESS — all state is in the checkpoint store.
     Any worker can pick up any partition from any checkpoint.
```

---

## 6. Deep Dive: Checkpoint Storage Backend

### PostgreSQL with Upsert

```sql
CREATE TABLE checkpoints (
  migration_id  VARCHAR(64),
  table_name    VARCHAR(256),
  partition_key VARCHAR(256),
  worker_id     VARCHAR(128),
  position      JSONB,
  stats         JSONB,
  updated_at    TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (migration_id, table_name, partition_key)
);

-- Atomic checkpoint write (upsert)
INSERT INTO checkpoints (migration_id, table_name, partition_key, worker_id, position, stats)
VALUES ('mig-7f3a', 'ORDERS', '2024-Q3', 'worker-5', '{"offset": 80000000}', '{"rows": 80000000}')
ON CONFLICT (migration_id, table_name, partition_key)
DO UPDATE SET
  worker_id = EXCLUDED.worker_id,
  position = EXCLUDED.position,
  stats = EXCLUDED.stats,
  updated_at = NOW();
```

**Why PostgreSQL over Redis?**
- Checkpoints must survive restarts — PostgreSQL's durability is critical.
- 100 writes/min is trivial for PostgreSQL (no performance concern).
- UPSERT semantics are exactly what we need.
- Redis persistence (AOF) can lose the last second of writes — unacceptable for checkpoints.

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Worker crash** | Partition stalls | GKE restarts; new worker resumes from checkpoint |
| **PostgreSQL down** | Can't write checkpoints | Workers buffer checkpoints locally; flush when PG recovers |
| **Checkpoint corruption** | Resume from wrong position | Checksums on checkpoint records; validate before resume |
| **Clock skew** | Watermark-based checkpoint inaccurate | Use source DB sequence numbers, not timestamps |
| **All workers crash simultaneously** | Full fleet restart | Orchestrator reads all checkpoints; relaunches entire fleet from last positions |
| **Orphaned partition (no worker assigned)** | Partition never completed | Orchestrator heartbeat monitor; reassign partitions with stale heartbeat |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Watermark over OFFSET | Row number offset | Index-based seek vs. full table scan; O(log n) vs O(n) |
| PostgreSQL over Redis | Redis with AOF | Durability > speed; 100 writes/min is trivial for PG |
| Per-partition checkpoints | Per-table checkpoints | Parallel workers need independent progress tracking |
| Write-after-commit | Write-before-commit | Prevents data loss; idempotent apply handles duplicates |
| 30-second interval | Per-row checkpoints | Balance between recovery granularity and checkpoint overhead |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "30 seconds of reprocessing on recovery — isn't that wasteful?" | "At most 30 seconds × throughput of one worker. If a worker processes 10K rows/sec, we re-process 300K rows. With MERGE INTO, this is idempotent and takes ~10 seconds. Compare that to re-processing 18 hours of work — 30 seconds of waste is a 99.95% savings." |
| "Why not checkpoint every row?" | "Checkpoint write has overhead (PG round-trip ~5ms). At 10K rows/sec, per-row checkpointing adds 50 seconds of latency per second — a 50x slowdown. Every 1K rows adds 50ms overhead — 0.5% — negligible." |
| "What about multi-table transactions?" | "For data migration, we process tables independently. Cross-table consistency is guaranteed by the CDC layer during the streaming phase, not the bulk load. The checkpointing system handles per-table progress." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **watermark-based distributed checkpointing system** backed by PostgreSQL. Each worker independently checkpoints its progress every 30 seconds or 1,000 rows using a monotonically increasing column (like primary key) as the watermark. Checkpoints are written AFTER the data is committed to Snowflake — never before — so recovery always produces correct results via idempotent MERGE INTO. Large tables are partitioned across workers with non-overlapping ranges, so each worker's checkpoint is independent. On crash, the orchestrator reads the latest checkpoint per partition and reassigns work to new pods. Recovery takes < 60 seconds and loses at most 30 seconds of work per worker."

---

## 11. Key Terms to Drop Naturally

- **Watermark**, **high-water mark**, **cursor**
- **Write-ahead checkpoint** (write after data commit)
- **Idempotent recovery**, **MERGE INTO**
- **Partition-based parallelism**
- **Checkpoint interval** (time vs. count)
- **Worker reassignment**, **stateless workers**
- **Exactly-once semantics**
