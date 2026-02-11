# Design a File Processing Pipeline

> **Interview Prompt:** "Design a system that takes a zip file and processes it through stages: Unzip → Parse → Convert → Repackage."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the max file size? | Streaming vs. buffer-all-in-memory |
| 2 | Are stages independent or do later stages need earlier results? | Determines pipeline coupling |
| 3 | Can individual files fail without failing the whole job? | Partial success handling |
| 4 | What's the throughput requirement? (files/sec, jobs/hour) | Worker pool sizing |
| 5 | Do we need to support different file formats beyond zip? | Extensibility of the unzip stage |

---

## 2. High-Level Architecture

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Ingest   │───▶│  Unzip   │───▶│  Parse   │───▶│ Convert  │───▶│ Repackage│
│  (Upload) │    │  Stage   │    │  Stage   │    │  Stage   │    │  Stage   │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
      │               │              │               │               │
      ▼               ▼              ▼               ▼               ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  S3      │    │  Queue   │    │  Queue   │    │  Queue   │    │  S3      │
│  (Input) │    │  Stage 2 │    │  Stage 3 │    │  Stage 4 │    │  (Output)│
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
                                      │
                                      ▼
                               ┌──────────────┐
                               │  Job Tracker  │
                               │  (State DB)   │
                               └──────────────┘
```

### Design Pattern: **Stage-Based Pipeline with Inter-Stage Queues**

Each stage is a **microservice** (or worker pool) connected by message queues. This gives us:
- **Independent scaling** per stage (parse might need 10x workers vs. unzip)
- **Failure isolation** (one stage crashing doesn't kill others)
- **Backpressure** (queues buffer when downstream is slower)

---

## 3. Deep-Dive: Each Pipeline Stage

### Stage 1: Ingest
```
Input: ZIP file (up to 10GB)
Output: S3 object reference

Steps:
  1. Accept resumable upload (multipart / tus)
  2. Stream to S3 (never buffer in memory)
  3. Validate checksum
  4. Emit event: { job_id, s3_ref } → Unzip Queue
```

### Stage 2: Unzip
```
Input: S3 reference to ZIP
Output: Individual files stored in S3, file manifest

Steps:
  1. Stream ZIP from S3
  2. For each entry:
     a. Extract to temp
     b. Upload to S3: s3://bucket/job-id/files/path/to/file.sql
     c. Record in manifest: { file_id, path, size, type }
  3. Emit N events to Parse Queue (one per file, or batched)
```

**Key decision:** Stream extraction — don't download the full ZIP to disk first.

### Stage 3: Parse
```
Input: S3 reference to a single file
Output: Parsed AST / metadata stored in S3

Steps:
  1. Download file from S3
  2. Detect file type (table DDL, view, procedure, etc.)
  3. Parse into structured representation
  4. Store parsed output + metadata
  5. Emit event → Convert Queue
```

**Scale independently:** If parsing is CPU-intensive, this stage gets the most workers.

### Stage 4: Convert
```
Input: Parsed representation
Output: Converted file in target format

Steps:
  1. Load parsed AST
  2. Apply transformation rules (source dialect → target dialect)
  3. Validate conversion output
  4. Store converted file in S3
  5. Emit event → Repackage Queue
```

### Stage 5: Repackage
```
Input: All converted files for a job
Output: ZIP file in S3, notification to user

Steps:
  1. Wait for all files in job to complete (or timeout)
  2. Stream-create output ZIP from S3 objects
  3. Generate summary report (success/failure counts)
  4. Store output ZIP in S3
  5. Notify user (webhook, email, UI update)
```

**Fan-in pattern:** This stage aggregates results from many Convert workers.

---

## 4. Handling Partial Failures

```
Job: 1000 files
├── 980 files: ✅ Converted successfully
├── 15 files:  ⚠️ Converted with warnings
└── 5 files:   ❌ Failed (moved to DLQ)

Result: Job marked as PARTIAL_SUCCESS
        Output ZIP contains 995 converted files
        Report lists 5 failed files with error details
```

**Policy options (configurable per tenant):**
- **Strict:** All-or-nothing. Any failure = job fails. 
- **Best-effort (recommended):** Deliver what succeeded, report failures.
- **Retry-then-skip:** Retry N times, then skip and report.

---

## 5. Scaling Strategy

| Stage | Bottleneck | Scaling Approach |
|-------|------------|------------------|
| Unzip | I/O bound (disk, network) | 1-2 workers per job (parallelism within stage is limited) |
| Parse | CPU bound | Auto-scale based on queue depth; 10-100 workers |
| Convert | CPU + possibly LLM API | Auto-scale; rate-limit LLM calls; batch when possible |
| Repackage | I/O bound, fan-in | 1 worker per job; runs after all conversions complete |

**Auto-scaling metric:** `queue_depth / processing_rate > threshold` → add workers.

### Capacity Estimation

```
Assumptions:
  100 jobs/hour, each with ~500 files
  Average file: 50KB source code
  Total: 50K files/hour = ~14 files/sec

Per-stage throughput:
  Unzip: 1 worker handles entire ZIP (~30 seconds for 500 files)
  Parse: 100ms/file → 1 worker = 10 files/sec → need 2 workers
  Convert: 500ms/file (with LLM) → need 7 workers
  Repackage: 1 worker per job (~15 seconds)

Queue sizing:
  Parse queue: 50K messages/hour, process in 2 seconds each
  Convert queue: 50K messages/hour, process in 500ms each
  Max backlog: 500 messages (1 job's worth)

Storage:
  S3: 50K files × 50KB × 3 copies (source, parsed, converted) = 7.5GB/hour
  Retention: 7 days → ~1.3TB active storage
```

### Observability

```
Metrics per stage:
  - Queue depth (messages waiting)
  - Processing rate (messages/sec)
  - Error rate (failures/sec)
  - Latency (p50, p95, p99 per stage)
  - Worker utilization (CPU, memory)

Job-level metrics:
  - End-to-end latency (upload → complete)
  - Success rate (% files converted)
  - Files/second throughput

Alerts:
  Queue depth > 1000 → scale up workers
  Error rate > 5% → page on-call
  Job duration > 2x average → investigate
```

---

## 6. Data Flow & State Tracking

```sql
CREATE TABLE pipeline_jobs (
    job_id       UUID PRIMARY KEY,
    tenant_id    UUID,
    status       ENUM('INGESTING','UNZIPPING','PROCESSING','REPACKAGING','COMPLETE','FAILED'),
    total_files  INT,
    stage_counts JSONB,  -- {"parsed": 100, "converted": 95, "failed": 5}
    input_ref    TEXT,
    output_ref   TEXT,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

CREATE TABLE pipeline_files (
    file_id      UUID PRIMARY KEY,
    job_id       UUID REFERENCES pipeline_jobs,
    stage        ENUM('UNZIPPED','PARSED','CONVERTED','FAILED'),
    file_path    TEXT,
    s3_refs      JSONB,  -- {"source": "...", "parsed": "...", "converted": "..."}
    error        TEXT,
    updated_at   TIMESTAMP
);
```

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just one monolithic service?" | "A monolithic pipeline can't scale stages independently. If parsing is 10x slower than unzipping, I'd waste resources scaling everything together. Queues between stages also provide natural backpressure and failure isolation." |
| "Queues between every stage adds latency" | "True — maybe 10-50ms per hop. For a job processing thousands of files over minutes, this is negligible. The benefit of independent scaling and fault isolation far outweighs the added latency." |
| "What if the repackage stage runs out of memory?" | "Repackaging streams files from S3 directly into the output ZIP. It never loads all files into memory — it reads and writes in chunks. For very large outputs, we can split into multiple ZIP volumes." |
| "How do you handle a ZIP bomb?" | "Pre-validate: check ZIP file headers for total uncompressed size before extracting. If uncompressed size exceeds a threshold (e.g., 100GB), reject with an error. Also limit the total number of files per ZIP (e.g., 50K)." |
| "What about poison files that crash the parser?" | "Isolate each file in a sandbox (container). If the parser crashes or times out, the file is moved to DLQ and the worker is recycled. Other files in the same job continue processing unaffected." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **stage-based pipeline** with inter-stage message queues. The five stages — Ingest, Unzip, Parse, Convert, Repackage — each run as independent worker pools. Queues (SQS or Kafka) between stages provide decoupling, backpressure, and fault isolation. Each stage scales independently based on queue depth — Parse and Convert typically need the most workers. All artifacts flow through S3, keeping workers stateless. A Job Tracker database records per-file progress, enabling partial success delivery and checkpointed resume. The Repackage stage uses a fan-in pattern, aggregating results once all files are processed."

---

## 9. Key Terms to Drop Naturally

- **Pipeline pattern**, **stage-based architecture**
- **Fan-out** (Unzip→Parse) and **fan-in** (Convert→Repackage)
- **Backpressure** via queues
- **Stateless workers**, **S3 as shared state**
- **Partial success**, **Dead Letter Queue**
- **Stream processing** (never buffer full file in memory)
- **ZIP bomb detection**, **sandbox isolation**
- **Queue depth-based auto-scaling**
