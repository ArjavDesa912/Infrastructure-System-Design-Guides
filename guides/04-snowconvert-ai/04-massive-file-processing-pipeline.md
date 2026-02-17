# Massive File Processing Pipeline

> **Interview Prompt:** "An enterprise client uploads a 50GB zip file containing 200,000 legacy Oracle PL/SQL files. Design a pipeline that unpacks, parses, converts to Snowflake SQL, validates, and repackages the output — in under 2 hours."

---

## 1. Requirements

### Functional
- Accept zip/tar uploads up to 100GB containing up to 500,000 files.
- Unpack, detect SQL dialect, parse AST, transpile to Snowflake, validate, and repackage as a downloadable artifact.
- Process files in parallel across a worker fleet.
- Provide real-time progress tracking (% complete, estimated time remaining).
- Support partial output — if 5% of files fail conversion, deliver the 95% that succeeded.
- Maintain file-level lineage: original file → converted file → validation report.

### Non-Functional
- **Throughput:** Process 200,000 files in < 2 hours → ~28 files/second sustained.
- **Scalability:** Support 10 concurrent enterprise batches.
- **Durability:** No file loss — every input file appears in the output (converted or error report).
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

## 2. API Design

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

## 3. High-Level Architecture

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
    │                                                           │
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

## 4. Deep Dive: Pipeline Phases

### 4.1 Phase 1: Streaming Unpack

**Problem:** You can't unzip a 50GB file into memory. You can't even unzip it to local disk — worker pods have limited ephemeral storage.

**Solution:** Stream the zip directly from GCS and extract files individually to GCS:

```
Streaming Unpack Architecture:

  GCS (input zip)                    GCS (unpacked files)
  ┌──────────┐                       ┌──────────────────┐
  │ batch.zip │ ── stream read ──▶   │ input/file001.sql│
  │  (50 GB)  │    one entry at      │ input/file002.sql│
  │           │    a time (no full   │ input/...        │
  │           │    unzip to disk)    │ input/file200K   │
  └──────────┘                       └──────────────────┘
                                              │
                                     Build manifest.json:
                                     [
                                       {"file_id": "f001", "path": "input/file001.sql",
                                        "size": 48230, "status": "PENDING"},
                                       {"file_id": "f002", "path": "input/file002.sql",
                                        "size": 12800, "status": "PENDING"},
                                       ...
                                     ]
```

```python
import zipfile
import io

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

### 4.2 Phase 2: Parallel Parse & Convert

**Fan-out architecture:**

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

**Worker process (per file):**

```
Worker.process_file(file_ref):
  1. Download source file from GCS (avg 500KB, < 100ms)
  2. Detect dialect if not specified (pattern matching on syntax)
  3. Parse AST:
     - Small file (< 1000 lines): in-memory, < 1s
     - Large file (> 5000 lines): chunk-based parsing, up to 15s
  4. Transpile AST from source dialect to Snowflake SQL
  5. Generate conversion report (warnings, unsupported constructs)
  6. Upload converted file to gs://pipeline/{batch}/converted/{file_id}.sf.sql
  7. Upload report to gs://pipeline/{batch}/reports/{file_id}.json
  8. Update manifest: status = CONVERTED | FAILED
```

### 4.3 Phase 3: Validation (Snowflake Dry-Run)

```
Validation Strategy:
  
  Option A: Syntax-only validation (fast, no Snowflake connection)
    → Use sqlglot or custom parser to check Snowflake SQL syntax
    → 1000 files/sec/worker
    → Catches: syntax errors, unknown functions, bad types
    → Misses: semantic errors (wrong column references, missing tables)
  
  Option B: Snowflake dry-run via EXPLAIN (accurate, requires connection)
    → Submit each converted file to Snowflake with EXPLAIN
    → Rate limited by Snowflake API: ~50 queries/sec per warehouse
    → Catches: everything — syntax, semantic, permission errors
    → Cost: Snowflake compute credits ($2/credit-hour)
  
  Hybrid approach (recommended):
    1. Fast syntax validation on all 200K files (2 minutes)
    2. Snowflake EXPLAIN on a random 5% sample (10K files, ~4 minutes)
    3. If sample pass rate < 95% → flag batch for review
    4. If sample pass rate ≥ 99% → mark batch as high-confidence
```

### 4.4 Phase 4: Repackage

```
Output Structure:
  output.zip/
  ├── converted/
  │   ├── schemas/
  │   │   ├── HR/
  │   │   │   ├── tables/
  │   │   │   ├── views/
  │   │   │   └── procedures/
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

## 5. Deep Dive: Work Distribution & Load Balancing

### 5.1 File Size Distribution Problem

Legacy codebases have highly skewed file sizes:

```
Typical distribution of 200K files:
  80% (160K files): < 100 lines,  avg processing time: 0.3s
  15% (30K files):  100-1000 lines, avg processing time: 2s
  4%  (8K files):   1000-5000 lines, avg processing time: 8s
  1%  (2K files):   > 5000 lines, avg processing time: 30s+
```

**Naive round-robin** gives one worker all the large files → that worker becomes the bottleneck.

**Solution: Two-pool worker strategy:**

```
Pool A: "Fast Workers" (80 pods, 2 CPU, 2GB RAM)
  → Process files < 1000 lines
  → High throughput, low resource

Pool B: "Heavy Workers" (20 pods, 8 CPU, 16GB RAM)
  → Process files > 1000 lines
  → More memory for large ASTs

Routing:
  Orchestrator checks file size from manifest
  → < 1000 lines: publish to fast-worker-topic
  → ≥ 1000 lines: publish to heavy-worker-topic
```

### 5.2 Backpressure & Flow Control

```
If Phase 2 (Convert) is faster than Phase 3 (Validate):
  → Converted files pile up in GCS
  → Validation workers can't keep up
  → Memory/storage pressure

Solution: Bounded pipeline with backpressure signals:
  - Each phase has a "pending work" gauge
  - If Phase 3 pending > 10,000 files → Phase 2 workers throttle (sleep 100ms between pulls)
  - If Phase 1 pending > 50,000 files → Unpack pauses
  
  Metric: pipeline_phase_pending_count{phase="validate", batch="batch-8f3a"}
```

---

## 6. Deep Dive: Resumability

### Checkpoint Design

```
Every 100 files processed, orchestrator writes checkpoint to PostgreSQL:

  checkpoint = {
    "batch_id": "batch-8f3a",
    "phase": "CONVERTING",
    "manifest_version": 47,
    "completed_file_ids": [sorted list or bitmap],
    "last_checkpoint_at": "2025-01-15T10:45:00Z",
    "workers_active": 85,
    "throughput_files_per_sec": 31.2
  }

On crash recovery:
  1. Read latest checkpoint from PostgreSQL
  2. Load manifest from GCS
  3. Diff: pending_files = manifest - completed_files
  4. Re-publish pending files to Pub/Sub
  5. Resume from exact position (no reprocessing)
```

> *This checkpoint-and-resume pattern mirrors the distributed checkpointing system I designed for VibeDB — where WAL (Write-Ahead Log) checkpoints allow crash recovery without replaying the entire log.*

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Corrupt zip file** | Unpack fails at entry N | Stream unpack catches per-entry errors; skip corrupt entries, log to report |
| **Worker OOM on large file** | Processing stalls | Route large files to heavy-worker pool; set per-file memory limit |
| **GCS rate limiting** | Upload/download throttled | Exponential backoff on GCS operations; spread across GCS prefixes |
| **Snowflake validation throttled** | Phase 3 bottleneck | Sample-based validation; batch EXPLAIN queries |
| **Pipeline crash at 60%** | Potential restart from 0% | Checkpoint every 100 files; resume from checkpoint |
| **Skewed file sizes** | Worker imbalance | Two-pool worker strategy; size-based routing |
| **Malicious file (zip bomb)** | OOM, disk exhaustion | Enforce max uncompressed size (10GB per file); stream extraction |

---

## 8. Trade-offs & Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| GCS for intermediate storage | Local disk across workers | Workers are stateless pods — local disk doesn't survive eviction; GCS gives durability + shared access |
| Pub/Sub per file (not per batch) | Single queue for batches | Per-file granularity gives natural load balancing and resumability |
| Two worker pools (fast/heavy) | Single pool with resource limits | Prevents large files from starving small files; optimizes resource allocation |
| Streaming unpack | Full unzip to disk | Can't fit 100GB on pod ephemeral storage; streaming uses constant memory |
| Hybrid validation (syntax + sampling) | Full Snowflake EXPLAIN on all files | EXPLAIN on 200K files costs significant compute time and Snowflake credits |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "50GB upload will time out." | "We use GCS resumable uploads — the client uploads directly to a signed GCS URL in chunks. If the connection drops, the client resumes from the last successful chunk. The API only receives the GCS reference after upload is complete." |
| "200K Pub/Sub messages is expensive." | "At $0.40 per million messages, 200K messages costs $0.08. The engineering simplicity of one-message-per-file — natural load balancing, easy retry, individual progress tracking — is worth 8 cents." |
| "Why not MapReduce?" | "MapReduce is batch-oriented with high startup latency (minutes). Our pipeline needs streaming progress updates and real-time resumability. Pub/Sub + workers gives us the parallelism of MapReduce with the interactivity of a streaming system." |
| "How do you handle dependencies between files?" | "Good question — stored procedures can reference tables that need to exist first. Phase 4 (Repackage) performs topological sort on the dependency graph to generate `_deploy_order.sql`. Individual file conversion is dependency-free; ordering only matters at deployment." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **four-phase streaming pipeline**: Unpack → Convert → Validate → Repackage. The 50GB zip is streamed from GCS entry-by-entry without loading into memory, producing a manifest of 200K files. The Orchestrator publishes one Pub/Sub message per file, consumed by two worker pools — fast workers for small files and heavy workers for large ASTs. Workers download, parse, transpile, and upload outputs to GCS in parallel across 100 pods. Validation uses a hybrid approach: fast syntax check on all files, Snowflake EXPLAIN on a 5% sample. Checkpoints are written to PostgreSQL every 100 files, enabling exact crash recovery. The final output is a structured zip with converted SQL, deployment ordering, and per-file conversion reports."

---

## 11. Key Terms to Drop Naturally

- **Streaming extraction**, **bounded memory**
- **Fan-out pattern**, **one-message-per-file**
- **Two-pool workers** (fast/heavy), **size-based routing**
- **Backpressure**, **flow control**
- **Checkpoint-and-resume**, **crash recovery**
- **Topological sort** (deployment ordering)
- **Resumable upload** (GCS signed URLs)
- **Manifest-driven pipeline**, **file-level lineage**
