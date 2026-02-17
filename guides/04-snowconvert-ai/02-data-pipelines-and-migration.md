# Part 2: Data Pipelines, Migration & Ingestion

> **Scope:** This guide consolidates four data-movement topics — massive file processing pipelines, change data capture (CDC), distributed checkpointing for crash recovery, and high-throughput data ingestion — into one reference. These are the "moving data at scale" problems interviewers love.

---

# Section A: Massive File Processing Pipeline

> **Interview Prompt:** "An enterprise client uploads a 50GB zip file containing 200,000 legacy Oracle PL/SQL files. Design a pipeline that unpacks, parses, converts to Snowflake SQL, validates, and repackages the output — in under 2 hours."

---

## A.1 Requirements

### Functional
- Accept zip/tar uploads up to 100GB containing up to 500,000 files.
- Unpack, detect SQL dialect, parse AST, transpile to Snowflake, validate, and repackage as a downloadable artifact.
- Process files in parallel across a worker fleet.
- Provide real-time progress tracking (% complete, estimated time remaining).
- Support partial output — if 5% of files fail, deliver the 95% that succeeded.
- Maintain file-level lineage: original file → converted file → validation report.

### Non-Functional
- **Throughput:** Process 200,000 files in < 2 hours → ~28 files/second sustained.
- **Scalability:** Support 10 concurrent enterprise batches.
- **Durability:** No file loss — every input file appears in the output.
- **Resumability:** If the pipeline crashes at 60%, resume from 60% (not restart from 0%).

### Capacity Estimation
```
Input: 50GB zip → ~100GB uncompressed (200K files, avg 500KB each)
Pipeline throughput needed: 200,000 files / 7,200 seconds = 28 files/sec
Worker capacity: avg 2 seconds/file/worker → 56 concurrent workers minimum
With overhead: 100 workers for safety margin

Storage:
  - Input staging (GCS): 100 GB
  - Output files: ~80 GB (converted SQL is often smaller)
  - Metadata/lineage: 200K rows × 2KB = 400 MB
  - Total working storage per batch: ~200 GB

Network:
  - Upload: 50GB over 10 min = ~700 Mbps (needs resumable upload)
  - Inter-worker GCS reads: 100 workers × 500KB/file × 0.5 files/sec = 25 MB/s
```

---

## A.2 API Design

### Submit Batch
```
POST /v1/batches
X-Idempotency-Key: <uuid>

{
  "upload_ref": "gs://uploads/tenant-42/batch-upload-001.zip",
  "source_dialect": "oracle_plsql",
  "target_dialect": "snowflake",
  "config": {
    "parallelism": "auto",
    "on_error": "continue",       // "continue" | "stop"
    "validation_level": "strict"   // "strict" | "lenient" | "skip"
  }
}

Response 202 Accepted:
{
  "batch_id": "batch-8f3a",
  "status": "UNPACKING",
  "estimated_duration_minutes": 95
}
```

### Get Batch Progress
```
GET /v1/batches/batch-8f3a/progress

{
  "batch_id": "batch-8f3a",
  "phase": "CONVERTING",
  "total_files": 200000,
  "completed": 142857,
  "failed": 312,
  "in_progress": 1200,
  "pending": 55631,
  "percent_complete": 71.4,
  "estimated_remaining_minutes": 28,
  "throughput_files_per_second": 31.2
}
```

---

## A.3 High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Pipeline Orchestrator                          │
│                   (GKE StatefulSet, 1 per batch)                     │
└──────────┬────────────┬────────────┬────────────┬───────────────────┘
           │            │            │            │
    ┌──────▼──────┐ ┌───▼──────┐ ┌──▼───────┐ ┌──▼──────────┐
    │   Phase 1    │ │  Phase 2  │ │  Phase 3  │ │   Phase 4    │
    │   UNPACK     │ │  PARSE &  │ │ VALIDATE  │ │  REPACKAGE   │
    │              │ │  CONVERT  │ │           │ │              │
    │ Streaming    │ │ Parallel  │ │ Snowflake │ │ Assembled    │
    │ extraction   │ │ workers   │ │ dry-run   │ │ output zip   │
    │ → file       │ │ pull from │ │ per file  │ │              │
    │   manifest   │ │ manifest  │ │           │ │              │
    └──────┬──────┘ └───┬──────┘ └──┬───────┘ └──┬──────────┘
           │            │            │            │
           ▼            ▼            ▼            ▼
    ┌──────────────────────────────────────────────────────────┐
    │                   GCS (Working Storage)                   │
    │  gs://pipeline/{batch_id}/                                │
    │  ├── input/          (unpacked source files)              │
    │  ├── converted/      (Snowflake SQL output)               │
    │  ├── validated/      (post-validation files)              │
    │  ├── reports/        (per-file conversion reports)        │
    │  ├── manifest.json   (file registry + status tracking)    │
    │  └── output.zip      (final deliverable)                  │
    └──────────────────────────────────────────────────────────┘
           │            │            │
           ▼            ▼            ▼
    ┌──────────────────────────────────────────────────────────┐
    │              PostgreSQL (Pipeline State)                  │
    │  - Batch status, phase transitions                       │
    │  - Per-file status (200K rows per batch)                 │
    │  - Checkpoint offsets for resumability                    │
    └──────────────────────────────────────────────────────────┘
```

---

## A.4 Deep Dive: Pipeline Phases

### A.4.1 Phase 1 — Streaming Unpack

**Problem:** You can't unzip a 50GB file into memory or to local disk — worker pods have limited ephemeral storage.

**Solution:** Stream the zip directly from GCS and extract files individually to GCS:

```python
import zipfile

def stream_unpack(gcs_zip_path: str, output_prefix: str) -> list:
    """Unpack zip from GCS without loading entire file into memory."""
    manifest = []
    
    with gcs.open(gcs_zip_path, 'rb') as zip_stream:
        with zipfile.ZipFile(zip_stream) as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue
                
                file_id = generate_file_id(entry.filename)
                output_path = f"{output_prefix}/input/{file_id}/{entry.filename}"
                
                # Stream each file individually to GCS
                with zf.open(entry) as source:
                    gcs.upload(output_path, source)
                
                manifest.append({
                    "file_id": file_id,
                    "original_name": entry.filename,
                    "gcs_path": output_path,
                    "size_bytes": entry.file_size,
                    "status": "PENDING"
                })
    
    return manifest
```

**Key insight:** This uses ~50MB of memory regardless of zip size. Each file is streamed from the zip entry directly to GCS.

> *This is directly analogous to how VibeDB handles large write batches — instead of buffering the entire batch in memory, we stream entries through the LSM memtable with bounded memory, flushing to SSTables when the buffer fills.*

### A.4.2 Phase 2 — Parallel Parse & Convert

```
                    ┌──────────────────────────────┐
                    │     File Manifest (200K)      │
                    │  ┌────┐┌────┐┌────┐         │
                    │  │ f1 ││ f2 ││ f3 │ ...     │
                    │  └──┬─┘└──┬─┘└──┬─┘         │
                    └─────│─────│─────│────────────┘
                          │     │     │
                    ┌─────▼──┐┌─▼────┐┌▼─────────┐
                    │Worker 1││Work 2 ││Worker 100│
                    │        ││       ││          │
                    │ 1.Read ││       ││          │
                    │   file ││       ││          │
                    │ 2.AST  ││       ││          │
                    │  parse ││       ││          │
                    │ 3.Trans││       ││          │
                    │  pile  ││       ││          │
                    │ 4.Write││       ││          │
                    │  output││       ││          │
                    └────────┘└───────┘└──────────┘

Work Distribution Strategy: Pub/Sub Message Per File
  - Orchestrator publishes 200K messages (one per file)
  - Workers pull messages in batches of 10
  - Natural load balancing: fast workers pull more messages
```

### A.4.3 Phase 3 — Validation (Snowflake Dry-Run)

```
Hybrid approach (recommended):
  1. Fast syntax validation on all 200K files (2 minutes)
  2. Snowflake EXPLAIN on a random 5% sample (10K files, ~4 minutes)
  3. If sample pass rate < 95% → flag batch for review
  4. If sample pass rate ≥ 99% → mark batch as high-confidence
```

| Validation | Speed | Accuracy | Cost |
|-----------|-------|----------|------|
| Syntax-only (sqlglot) | 1000 files/sec | Catches syntax errors | Free |
| Snowflake EXPLAIN | 50 queries/sec per warehouse | Catches everything | Snowflake credits |
| Hybrid (syntax + 5% sample) | Fast with safety | Best tradeoff | Minimal credits |

### A.4.4 Phase 4 — Repackage

```
Output Structure:
  output.zip/
  ├── converted/
  │   ├── schemas/
  │   │   ├── HR/ (tables/, views/, procedures/)
  │   │   └── FINANCE/
  │   ├── standalone/
  │   └── _deploy_order.sql  (topologically sorted deployment script)
  ├── reports/
  │   ├── summary.json       (aggregate stats)
  │   ├── per_file/          (individual conversion reports)
  │   └── unsupported.csv    (files that couldn't be converted)
  └── metadata/
      ├── lineage.json       (input → output mapping)
      └── config.json        (conversion settings used)
```

---

## A.5 Deep Dive: Work Distribution & Load Balancing

### File Size Distribution Problem

```
Typical distribution of 200K files:
  80% (160K files): < 100 lines,  avg processing time: 0.3s
  15% (30K files):  100-1000 lines, avg processing time: 2s
  4%  (8K files):   1000-5000 lines, avg processing time: 8s
  1%  (2K files):   > 5000 lines, avg processing time: 30s+
```

**Two-pool worker strategy:**

```
Pool A: "Fast Workers" (80 pods, 2 CPU, 2GB RAM)
  → Process files < 1000 lines
  → High throughput, low resource

Pool B: "Heavy Workers" (20 pods, 8 CPU, 16GB RAM)
  → Process files > 1000 lines
  → More memory for large ASTs

Routing: Orchestrator checks file size from manifest
  → < 1000 lines: publish to fast-worker-topic
  → ≥ 1000 lines: publish to heavy-worker-topic
```

### Backpressure & Flow Control

```
If Phase 2 (Convert) is faster than Phase 3 (Validate):
  → Converted files pile up in GCS → memory/storage pressure

Solution: Bounded pipeline with backpressure signals:
  - Each phase has a "pending work" gauge
  - If Phase 3 pending > 10,000 files → Phase 2 workers throttle
  - If Phase 1 pending > 50,000 files → Unpack pauses
  
  Metric: pipeline_phase_pending_count{phase="validate", batch="batch-8f3a"}
```

---

## A.6 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Corrupt zip file** | Unpack fails at entry N | Stream unpack catches per-entry errors; skip + log |
| **Worker OOM on large file** | Processing stalls | Route large files to heavy-worker pool |
| **GCS rate limiting** | Upload/download throttled | Exponential backoff; spread across GCS prefixes |
| **Pipeline crash at 60%** | Potential restart from 0% | Checkpoint every 100 files; resume from checkpoint |
| **Skewed file sizes** | Worker imbalance | Two-pool worker strategy; size-based routing |
| **Malicious file (zip bomb)** | OOM, disk exhaustion | Enforce max uncompressed size (10GB per file); stream extraction |

---

## A.7 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| GCS for intermediate storage | Local disk | Workers are stateless — local disk doesn't survive eviction |
| Pub/Sub per file (not per batch) | Single queue | Per-file gives natural load balancing and resumability |
| Two worker pools (fast/heavy) | Single pool | Prevents large files from starving small files |
| Streaming unpack | Full unzip to disk | Can't fit 100GB on pod ephemeral storage |
| Hybrid validation | Full EXPLAIN on all files | 200K EXPLAIN queries costs significant time and credits |

---

## A.8 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "50GB upload will time out." | "GCS resumable uploads — client uploads directly to a signed GCS URL in chunks. If connection drops, resume from last chunk. The API only receives the GCS reference after upload is complete." |
| "200K Pub/Sub messages is expensive." | "At $0.40 per million messages, 200K messages costs $0.08. The engineering simplicity of one-message-per-file — natural load balancing, easy retry, individual progress tracking — is worth 8 cents." |
| "Why not MapReduce?" | "MapReduce has high startup latency (minutes) and is batch-oriented. Our pipeline needs streaming progress updates and real-time resumability. Pub/Sub + workers gives us MapReduce parallelism with streaming interactivity." |
| "How do you handle dependencies between files?" | "Stored procedures can reference tables that need to exist first. Phase 4 performs topological sort on the dependency graph to generate `_deploy_order.sql`. Individual file conversion is dependency-free; ordering only matters at deployment." |

---

# Section B: Change Data Capture (CDC) Pipeline

> **Interview Prompt:** "A Fortune 500 company is migrating their Oracle production database to Snowflake. They can't afford downtime. Design a zero-downtime CDC pipeline that keeps Snowflake in sync during migration."

---

## B.1 Requirements

### Functional
- Capture all changes (INSERT, UPDATE, DELETE) from Oracle in real time.
- Apply changes to Snowflake with < 30-second replication lag.
- Support initial bulk load of TBs without blocking production.
- Handle schema evolution during migration.
- Provide zero-data-loss cutover mechanism.

### Non-Functional
- **Latency:** P99 replication lag < 30 seconds.
- **Throughput:** 50,000 CDC events/second at peak.
- **Durability:** Every committed Oracle transaction must appear in Snowflake.
- **Consistency:** Transactional ordering preserved per table.

### Capacity Estimation
```
Source database:      10 TB, 500 tables, 2B rows
Transaction rate:     5,000 TPS (peak 50K TPS)
Avg CDC event size:   500 bytes
CDC throughput:       50K × 500B = 25 MB/sec peak
Daily CDC volume:     ~216 GB/day
Kafka retention (7d): ~1.5 TB
Initial bulk load:    10 TB at 500 MB/sec ≈ 5.5 hours
```

---

## B.2 Architecture

```
┌────────────────────────────────────────────────┐
│          Source: Oracle Production               │
│  ┌──────────────────────────┐                   │
│  │  Redo Logs (WAL stream)  │──────┐            │
│  └──────────────────────────┘      │            │
└────────────────────────────────────│────────────┘
                                     │
┌────────────────────────────────────▼────────────┐
│         CDC Capture: Debezium on GKE            │
│  - Reads Oracle LogMiner / XStream              │
│  - Converts redo entries to CDC events          │
│  - Publishes to Kafka with exactly-once         │
│  - Tracks position via Oracle SCN               │
└────────────────────────────────────┬────────────┘
                                     │
┌────────────────────────────────────▼────────────┐
│              Kafka (Event Bus)                   │
│  Topic per table: cdc.oracle.HR.EMPLOYEES        │
│  Partitioned by primary key                      │
│  Event: {op: "u", before: {...}, after: {...}}  │
└────────────────────────────────────┬────────────┘
                                     │
┌────────────────────────────────────▼────────────┐
│       Sink Connector: Kafka → Snowflake         │
│  - Micro-batch consumption (10s windows)         │
│  - Stage events as JSON to internal stage        │
│  - MERGE INTO for idempotent upserts             │
│  - Commit Kafka offsets after Snowflake COMMIT   │
└────────────────────────────────────┬────────────┘
                                     │
┌────────────────────────────────────▼────────────┐
│           Sink: Snowflake Database               │
│  MIGRATION_TARGET.HR.EMPLOYEES                   │
│  MIGRATION_TARGET.FINANCE.TRANSACTIONS           │
└─────────────────────────────────────────────────┘
```

---

## B.3 Deep Dive: CDC Capture Strategies

### Log-Based CDC (Recommended)

```
Transaction: UPDATE employees SET salary = 80000 WHERE id = 42;

Oracle Redo Log Entry:
  SCN: 982347123
  Table: HR.EMPLOYEES
  Op: UPDATE
  Before: {id: 42, salary: 75000}
  After:  {id: 42, salary: 80000}

→ Debezium reads via LogMiner API → publishes to Kafka
```

| CDC Method | Pros | Cons |
|-----------|------|------|
| **Log-based** | Zero source overhead; captures all ops | Requires DBA access |
| **Trigger-based** | Simple; any DB | Doubles write load |
| **Query-based** | No privileges needed | Misses deletes; high latency |

**Why log-based?** At 5,000 TPS, triggers double write load on production.

### Exactly-Once Apply via MERGE

```sql
MERGE INTO target_table t
USING staging_table s ON t.pk = s.pk
WHEN MATCHED AND s.op = 'u' THEN
  UPDATE SET t.col1 = s.after_col1, ...
WHEN MATCHED AND s.op = 'd' THEN
  DELETE
WHEN NOT MATCHED AND s.op IN ('c', 'u') THEN
  INSERT (col1, ...) VALUES (s.after_col1, ...);
```

MERGE is inherently idempotent — applying the same event twice produces the same result.

### Schema Evolution

```
Oracle DDL: ALTER TABLE employees ADD COLUMN department_id NUMBER;

Sink Handler (auto-evolve mode):
  1. Detect schema change event
  2. Map Oracle NUMBER → Snowflake NUMBER
  3. Execute: ALTER TABLE EMPLOYEES ADD COLUMN DEPARTMENT_ID NUMBER;
  4. Resume CDC with new schema

Modes: "evolve" (auto), "pause" (human review), "fail" (strict)
```

---

## B.4 Deep Dive: Initial Load + CDC Consistency

```
The Challenge:
  T0: Start snapshot → reads EMPLOYEES (old data)
  T1: Production UPDATE to EMPLOYEES during snapshot
  T2: Snapshot reads ORDERS (new data) → INCONSISTENT!

Solution: Consistent snapshot at a specific SCN:
  1. initial_scn = GET_CURRENT_SCN()  → 982340000
  2. Export ALL tables AS OF SCN 982340000 (consistent point-in-time)
  3. Bulk load into Snowflake via COPY INTO from GCS
  4. Start CDC from SCN 982340000
     → No gap, no overlap, no inconsistency
```

---

## B.5 Deep Dive: Zero-Downtime Cutover

```
Phase 1: Preparation (hours before)
  ├── Verify replication lag < 5 seconds
  ├── Run row count and checksum validation
  └── Notify stakeholders

Phase 2: Quiesce (30-second window)
  ├── Set Oracle READ-ONLY (or block writes at app layer)
  ├── Wait for all CDC events to drain to Snowflake
  ├── Verify: Oracle max(SCN) == Snowflake applied max(SCN)
  └── Final row count match validation

Phase 3: Switch (< 5 seconds)
  ├── Update DNS / connection pool to point to Snowflake
  └── Monitor error rates for 15 minutes

Phase 4: Rollback Window (72 hours)
  ├── Keep Oracle in read-only mode
  ├── Keep reverse CDC running (Snowflake → Oracle)
  └── After 72 hours → decommission Oracle pipeline
```

---

## B.6 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Debezium crashes** | CDC stops | GKE restarts; resumes from last Kafka Connect offset |
| **Kafka broker failure** | Delivery paused | 3-broker cluster, replication factor 3 |
| **Snowflake warehouse suspended** | Sink stalls | Alert on lag; auto-resume on CDC activity |
| **Schema change breaks MERGE** | Type mismatch | Schema evolution handler + pause mode |
| **Oracle redo log rotation** | CDC falls behind | Configure archive log retention > lag |
| **Network partition** | Capture stalls | Debezium auto-reconnects; heartbeat detects staleness |

---

## B.7 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Debezium | Oracle GoldenGate | Open-source, Kafka-native, no licensing |
| Kafka as bus | Direct to Snowflake | Durability, replay, decoupling, multiple consumers |
| MERGE INTO | Row-by-row ops | Idempotent, batch-efficient, handles all op types |
| Micro-batch (10s) | Snowpipe Streaming (1s) | 10s sufficient for migration; 5x cheaper |
| SCN-based snapshot | Lock-based | Non-blocking; locks halt production |

---

## B.8 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not big-bang weekend migration?" | "For Fortune 500, 4 hours downtime costs millions. CDC allows continuous replication with a 30-second cutover. Infrastructure complexity is justified by business risk reduction." |
| "What if CDC lag grows?" | "Alert at 30s, page at 2min. Root causes: undersized warehouse (scale up), Kafka consumer lag (add partitions), network saturation (compress). Per-table lag dashboard isolates bottleneck." |
| "Data types that don't map?" | "Type mapping registry: Oracle NUMBER(38)→Snowflake NUMBER(38), DATE→TIMESTAMP_NTZ, CLOB→VARCHAR(16MB). Use widest compatible type for ambiguous mappings." |
| "Data corruption after cutover?" | "72-hour rollback window with Oracle in read-only + reverse CDC. Revert DNS in seconds. Row checksums isolate corrupt data." |

---

# Section C: Distributed Checkpointing System

> **Interview Prompt:** "A terabyte-scale data migration is 18 hours into a 20-hour run when a GKE node crashes. Design a checkpointing system that lets the migration resume from where it left off."

---

## C.1 Requirements

### Functional
- Periodically persist migration progress so work can resume after any failure.
- Support both file-level and row-level checkpoint granularity.
- Guarantee exactly-once semantics: no data duplicated or skipped on resume.
- Support concurrent checkpoint writes from multiple workers.
- Provide checkpoint inspection/management API.

### Non-Functional
- **Checkpoint frequency:** Every 30 seconds or every 1,000 records, whichever comes first.
- **Checkpoint write latency:** < 100ms P99.
- **Recovery time:** Resume within 60 seconds of a crash.
- **Storage overhead:** < 1% of total migration data size.

### Capacity Estimation
```
Migration size:       1 TB, 500 tables, 2B total rows
Migration duration:   ~20 hours
Workers:              50 concurrent workers
Checkpoint frequency: Every 30s per worker → 50 × 2/min = 100 checkpoints/min
Checkpoint record:    ~2 KB
Total checkpoints:    100/min × 1,200 min = 120K checkpoints
Storage:              120K × 2 KB = ~240 MB (negligible)
```

---

## C.2 Architecture

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
│  1. SELECT * FROM checkpoints WHERE migration_id='mig-7f3a│
│  2. For each table: resume from MAX(offset)               │
│  3. Relaunch workers with resume positions                │
└──────────────────────────────────────────────────────────┘
```

---

## C.3 Deep Dive: Checkpoint Strategies

### Row-Offset vs. Watermark-Based (Data Migration)

**Offset-based (naive):**
```
Resume: SELECT * FROM ORDERS WHERE order_date = '2024-06' ORDER BY rowid OFFSET 45000000
→ SLOW: full scan to skip 45M rows
```

**Watermark-based (recommended):**
```
Checkpoint: watermark = last processed order_id = 987654321

Resume: SELECT * FROM ORDERS WHERE order_date = '2024-06' AND order_id > 987654321
                                                             ORDER BY order_id
→ Uses index on order_id → instant seek, no full scan
```

> *This watermark-based checkpoint is analogous to how VibeDB's compaction process tracks progress — the cursor stores the last key processed, and on restart seeks to that key via the sparse index.*

### File-Level Checkpointing (Code Migration)

```
200,000 SQL files to convert, tracked in manifest:
  f-142857: COMPLETED
  f-142858: RUNNING   ← Worker was here when crash happened
  f-142859: PENDING
  
Recovery: Find all files with status != COMPLETED → re-publish to Pub/Sub
```

### Checkpoint Consistency: Write-After-Commit

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

  NEVER: Write checkpoint BEFORE data commit
  → That would cause data loss (checkpoint says "done" but data isn't there)
```

---

## C.4 Multi-Worker Coordination

### Table Partitioning for Parallelism

```
Table ORDERS (1.2B rows) split across 4 workers:

  Worker 1: WHERE order_date BETWEEN '2024-01' AND '2024-03'
  Worker 2: WHERE order_date BETWEEN '2024-04' AND '2024-06'
  Worker 3: WHERE order_date BETWEEN '2024-07' AND '2024-09'
  Worker 4: WHERE order_date BETWEEN '2024-10' AND '2024-12'

Each worker maintains its own checkpoint independently.
No coordination needed — partitions are non-overlapping.
```

### Worker Failure & Reassignment

```
Worker 3 crashes:
  1. GKE detects pod failure (liveness probe)
  2. Checkpoint for worker-3: partition=2024-Q3, offset=80M
  3. Orchestrator reassigns partition 2024-Q3 to new Worker 5
  4. Worker 5 reads checkpoint, resumes from offset 80M
  5. Other workers unaffected

Key: Workers are STATELESS — all state is in the checkpoint store.
     Any worker can pick up any partition from any checkpoint.
```

### Checkpoint Storage: PostgreSQL with Upsert

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
DO UPDATE SET worker_id = EXCLUDED.worker_id, position = EXCLUDED.position,
              stats = EXCLUDED.stats, updated_at = NOW();
```

**Why PostgreSQL over Redis?** Checkpoints must survive restarts. 100 writes/min is trivial for PG. Redis AOF can lose the last second — unacceptable.

---

## C.5 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Worker crash** | Partition stalls | GKE restarts; new worker resumes from checkpoint |
| **PostgreSQL down** | Can't write checkpoints | Workers buffer locally; flush when PG recovers |
| **Checkpoint corruption** | Resume from wrong position | Checksums on records; validate before resume |
| **All workers crash** | Full fleet restart | Orchestrator reads all checkpoints; relaunches from last positions |
| **Orphaned partition** | Never completed | Heartbeat monitor; reassign stale partitions |

---

## C.6 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "30 seconds of reprocessing — wasteful?" | "At most 30s × one worker's throughput. If 10K rows/sec, we re-process 300K rows. MERGE is idempotent, takes ~10 seconds. Compare to re-processing 18 hours — 30s waste is 99.95% savings." |
| "Why not checkpoint every row?" | "PG round-trip ~5ms. At 10K rows/sec, per-row checkpointing adds 50s overhead per second — 50x slowdown. Every 1K rows adds 50ms — negligible." |
| "Multi-table transactions?" | "Data migration processes tables independently. Cross-table consistency is guaranteed by CDC during streaming, not bulk load." |

---

# Section D: High-Throughput Data Ingestion

> **Interview Prompt:** "SnowConvert needs to ingest terabytes of legacy database exports — CSV, Parquet, Oracle Data Pump — into Snowflake as fast as possible. Design a high-throughput ingestion pipeline."

---

## D.1 Requirements

### Functional
- Ingest from multiple formats: CSV, Parquet, Avro, Oracle DMP, SQL Server BCP.
- Load into Snowflake with schema mapping and type conversion.
- Support bulk loads (TBs) and incremental daily loads (GBs).
- File-level tracking and data validation (row counts, checksums).

### Non-Functional
- **Throughput:** 1 TB/hour sustained for bulk loads.
- **Latency:** Incremental loads available within 60 seconds.
- **Reliability:** Zero data loss.
- **Cost efficiency:** Minimize Snowflake credit consumption.

### Capacity Estimation
```
Bulk load:
  Data volume:         10 TB
  Target time:         10 hours → 1 TB/hour → 278 MB/sec
  Snowflake warehouse: XL (16 nodes) → ~250 MB/sec sustained
  Cost:                10 × 16 credits/hr × $2/credit = $320

Incremental load:
  Daily volume:        50 GB (500 files)
  Snowflake warehouse: Medium → sufficient
  Cost:                $60/month
```

---

## D.2 Architecture

```
┌──────────────────────────────────────────────────┐
│    External Data Sources (Oracle DMP, BCP, CSV)   │
└───────────────┬──────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────┐
│        Format Normalizer (GKE Pods)               │
│  - Converts all formats → Parquet                 │
│  - Splits into optimal 256MB chunks               │
│  - Infers/validates schema                        │
└───────────────┬──────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────┐
│        GCS Staging Area (co-located region)       │
│  Distributed prefixes to avoid hotspots           │
└───────────────┬──────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────┐
│        Snowflake Loader                           │
│  Bulk:    COPY INTO with XL warehouse             │
│  Stream:  Snowpipe (event-driven, 30-60s)         │
│  RT:      Snowpipe Streaming (sub-second)         │
└──────────────────────────────────────────────────┘
```

---

## D.3 Deep Dive: Performance Optimization

### File Sizing (Critical for Throughput)

```
Too small (< 10 MB): Overhead per file dominates → ~50 MB/sec ❌
Too large (> 1 GB):  Can't parallelize within file → ~100 MB/sec ❌
Optimal (100-250 MB compressed): Full parallelism → 250+ MB/sec ✅

Rule: Number of files ≥ warehouse nodes × 4
  XL (16 nodes): need ≥ 64 files for full utilization
```

### Format Selection

| Format | Compression | When to Use |
|--------|-------------|-------------|
| **Parquet** | Snappy (3-5x) | Default for bulk — columnar, fast, typed |
| **CSV** | Gzip (5-10x) | Legacy sources only |
| **Avro** | Deflate | Kafka CDC events |
| **JSON** | Gzip | Nested/dynamic schemas → VARIANT column |

**Why Parquet?** Columnar → Snowflake reads only needed columns. Built-in schema. Snappy compression. Type-safe.

### Parallel Loading

```sql
-- Snowflake distributes files across warehouse nodes automatically
COPY INTO HR.EMPLOYEES
FROM @staging_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- XL: 16 nodes × ~15 MB/sec each = ~250 MB/sec aggregate
```

---

## D.4 Deep Dive: Type Mapping (Oracle → Snowflake)

```
Oracle                    Snowflake               Notes
──────────────────────────────────────────────────────────
NUMBER(38)               NUMBER(38,0)             Exact
VARCHAR2(4000)           VARCHAR(4000)            Simple rename
CLOB                     VARCHAR(16777216)        16MB max in SF
BLOB                     BINARY                   Or store in stage
DATE                     TIMESTAMP_NTZ            Oracle DATE has time
TIMESTAMP WITH TZ        TIMESTAMP_TZ             Direct mapping
RAW(16)                  BINARY(16)               UUID common case
XMLTYPE                  VARIANT                  Parse XML to JSON
SDO_GEOMETRY             GEOGRAPHY                Spatial data

Problematic:
  LONG     → VARCHAR(16MB)      Deprecated in Oracle
  BFILE    → Extract to stage   External file reference
  USER_TYPE→ Flatten to columns Custom types
```

---

## D.5 Deep Dive: Data Validation

```
Phase 1: Pre-Load (in GCS staging)
  - File integrity: checksum verification
  - Schema validation: columns/types match target
  - Data sampling: 1% of rows for corruption check

Phase 2: Post-Load (in Snowflake)
  SELECT COUNT(*) source vs target → row count match
  SELECT HASH_AGG(*) → checksum comparison

Phase 3: Ongoing (business logic)
  SELECT COUNT(*) FROM HR.EMPLOYEES WHERE salary < 0;  -- Should be 0
```

---

## D.6 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Corrupt Parquet file** | COPY skips/fails | `ON_ERROR = CONTINUE` + error log |
| **Type conversion failure** | Rows rejected | `VALIDATION_MODE = RETURN_ERRORS` → fix → retry |
| **Warehouse timeout** | Long COPY killed | Split into smaller batches (1000 files each) |
| **GCS throttled** | Slow reads | Distribute prefixes; exponential backoff |
| **Schema mismatch** | COPY fails | Pre-validate; MATCH_BY_COLUMN_NAME |

---

## D.7 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why normalize to Parquet?" | "CSV is schemaless — type errors surface at load time, wasting expensive Snowflake compute. Parquet conversion on cheap GKE pods catches mismatches before we spin up the warehouse. Also 3-5x better compression." |
| "1 TB/hour seems slow" | "That's with XL at $32/hour. 4XL (128 nodes) reaches 4 TB/hour at $128/hour. The constraint is cost, not capability." |
| "Schema changes between loads?" | "MATCH_BY_COLUMN_NAME maps by name, not position. New columns trigger schema evolution (auto ALTER TABLE or alert for review)." |

---

## Summary Narratives

### File Processing
> "A **four-phase streaming pipeline**: Unpack → Convert → Validate → Repackage. The zip is streamed entry-by-entry with bounded memory. Pub/Sub per file across two worker pools (fast/heavy). Hybrid validation: syntax check all, EXPLAIN 5% sample. Checkpoint every 100 files for crash recovery."

### CDC Pipeline
> "A **log-based CDC pipeline** using Debezium on GKE → Kafka (topic per table) → Snowflake via MERGE INTO in 10s micro-batches. SCN-consistent snapshot for initial load. 30-second quiesce cutover with 72-hour rollback window."

### Checkpointing
> "A **watermark-based distributed checkpointing system** backed by PostgreSQL. Workers checkpoint every 30s using a monotonically increasing column. Checkpoints written AFTER data commit. Recovery in < 60 seconds, losing at most 30s of work."

### Data Ingestion
> "A **three-stage pipeline**: Normalize (all formats → Parquet, 256MB chunks) → Stage (co-located GCS) → Load (COPY INTO with XL warehouse at 250 MB/sec). Snowpipe for incremental. Type mapping registry for Oracle → Snowflake. Post-load validation with row counts and checksums."

---

## Key Terms to Drop Naturally

- **Streaming extraction**, **bounded memory**, **fan-out pattern**
- **Two-pool workers** (fast/heavy), **size-based routing**
- **Backpressure**, **flow control**, **checkpoint-and-resume**
- **Topological sort** (deployment ordering), **resumable upload**
- **CDC**, **log-based capture**, **Debezium**, **SCN**
- **MERGE INTO**, **idempotent upsert**, **micro-batch**
- **Zero-downtime cutover**, **quiesce window**, **replication lag**
- **Schema evolution**, **type mapping registry**
- **Watermark**, **high-water mark**, **write-after-commit**
- **Partition-based parallelism**, **stateless workers**
- **COPY INTO**, **Snowpipe**, **Snowpipe Streaming**
- **Parquet**, **columnar format**, **Snappy compression**
- **File sizing** (256MB optimal), **GCS prefix distribution**
- **MATCH_BY_COLUMN_NAME**, **ON_ERROR = CONTINUE**
- **Co-located storage**, **credit consumption**, **warehouse sizing**
