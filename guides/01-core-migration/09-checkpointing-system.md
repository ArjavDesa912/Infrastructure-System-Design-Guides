# Design a Checkpointing System

> **Interview Prompt:** "If a 5-hour migration fails at 99%, how do we resume without starting over?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the unit of work? (per file, per batch, per SQL statement?) | Determines checkpoint granularity |
| 2 | Is the work idempotent? (can we safely re-run a unit?) | Affects how precise checkpoints need to be |
| 3 | What causes failures? (crashes, OOM, network, bugs?) | Shapes recovery strategy |
| 4 | How frequently should we checkpoint? (every item, every N items?) | Performance vs. recovery granularity trade-off |
| 5 | Do we need to support rollback, or only resume-forward? | Complexity of the state machine |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Migration Job                      │
│                                                       │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐     │
│  │ File 1 │  │ File 2 │  │ File 3 │  │File N  │     │
│  │   ✅   │  │   ✅   │  │   ✅   │  │   ⬜   │     │
│  └────────┘  └────────┘  └────────┘  └────────┘     │
│       │           │           │           │          │
│       ▼           ▼           ▼           ▼          │
│  ┌─────────────────────────────────────────────┐     │
│  │           Checkpoint Store                   │     │
│  │  ┌──────────────────────────────────┐       │     │
│  │  │ file_1: DONE   | output: s3://.. │       │     │
│  │  │ file_2: DONE   | output: s3://.. │       │     │
│  │  │ file_3: DONE   | output: s3://.. │       │     │
│  │  │ file_4: RUNNING| attempt: 2      │       │     │
│  │  │ file_5: PENDING                  │ ← resume   │
│  │  │ ...                              │   from here │
│  │  └──────────────────────────────────┘       │     │
│  └─────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────┘
```

---

## 3. Deep-Dive: Checkpoint Design

### 3.1 Checkpoint Granularity

| Granularity | Example | Recovery Precision | Overhead |
|-------------|---------|-------------------|----------|
| **Per-job** | "Migration 50% done" | Restart from ~50% | Very low |
| **Per-file** (recommended) | "File 4,872 of 10,000" | Restart from exact file | Low |
| **Per-statement** | "Line 47 of procedure" | Restart from exact line | Moderate |
| **Per-byte** | "Offset 1,048,576" | Restart from exact position | High |

**Per-file is the sweet spot** for migration workloads. Each file is typically small and independent.

### 3.2 Checkpoint State Machine

```
   PENDING ──▶ RUNNING ──▶ COMPLETED
                  │
                  ▼
               FAILED ──▶ RETRYING ──▶ COMPLETED
                  │                       │
                  ▼                       ▼
              DLQ (max retries)      COMPLETED
```

### 3.3 Checkpoint Storage

```sql
CREATE TABLE checkpoints (
    job_id          UUID,
    item_id         UUID,         -- file_id, task_id, etc.
    item_key        TEXT,         -- human-readable identifier (file path)
    status          ENUM('PENDING','RUNNING','COMPLETED','FAILED','DLQ'),
    attempt_count   INT DEFAULT 0,
    worker_id       UUID,
    input_ref       TEXT,         -- S3 URI to input
    output_ref      TEXT,         -- S3 URI to output
    error_message   TEXT,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    PRIMARY KEY (job_id, item_id)
);

-- Index for efficient resume queries
CREATE INDEX idx_checkpoint_resume 
    ON checkpoints(job_id, status) 
    WHERE status IN ('PENDING', 'FAILED');
```

### 3.4 Resume Algorithm

```python
def resume_migration(job_id):
    # 1. Load checkpoint state
    completed = db.query("SELECT item_id FROM checkpoints 
                         WHERE job_id = ? AND status = 'COMPLETED'", job_id)
    
    # 2. Get full item list
    all_items = get_all_items(job_id)
    
    # 3. Determine remaining work
    remaining = all_items - completed
    
    # 4. Re-queue RUNNING items (worker may have crashed)
    stale_running = db.query("SELECT item_id FROM checkpoints 
                             WHERE job_id = ? AND status = 'RUNNING'
                             AND started_at < NOW() - INTERVAL '10 minutes'", job_id)
    remaining = remaining | stale_running
    
    # 5. Reset failed items for retry (if under max retries)
    retryable = db.query("SELECT item_id FROM checkpoints 
                         WHERE job_id = ? AND status = 'FAILED'
                         AND attempt_count < max_retries", job_id)
    remaining = remaining | retryable
    
    # 6. Enqueue remaining items
    for item in remaining:
        queue.send(item)
    
    log(f"Resuming {len(remaining)} of {len(all_items)} items")
```

---

## 4. Checkpoint Strategies

### 4.1 Write-Ahead Checkpointing
```
1. Write intent to checkpoint store: "About to process file X"
2. Process file X
3. Update checkpoint: "File X completed with output Y"
```
- If crash between 1 and 3 → we know file X was in-progress → re-run it
- **Most reliable,** small write overhead

### 4.2 Periodic Batch Checkpointing
```
Process 100 files → Batch write checkpoint for all 100
```
- Lower overhead (fewer DB writes)
- Risk: up to 100 files re-processed on crash
- Good when work is idempotent

### 4.3 Event-Sourced Checkpointing
```
Append events: FILE_STARTED, FILE_COMPLETED, FILE_FAILED
Reconstruct state by replaying events
```
- Full audit trail
- Can compute "state at any point in time"
- Higher storage, more complex resume logic

---

## 5. Handling Long-Running Items

What if a single file takes 30 minutes to convert?

```
┌─────────────────────────────────────────────────┐
│ File: complex_procedure.sql                      │
│                                                   │
│ Sub-checkpoints:                                  │
│  ├── Parse:    ✅ (checkpoint saved)              │
│  ├── Analyze:  ✅ (checkpoint saved)              │
│  ├── Convert:  ⬜ (in progress, heartbeat active) │
│  └── Validate: ⬜ (pending)                       │
│                                                   │
│ Last heartbeat: 2 seconds ago ← still alive       │
└─────────────────────────────────────────────────┘
```

- **Sub-checkpoints** within large items
- **Heartbeat** to signal liveness (prevents premature timeout)
- **Intermediate results** saved to S3 (parse output, analysis output)

---

## 6. Consistency Guarantees

| Guarantee | How to Achieve |
|-----------|----------------|
| **No data loss** | Write-ahead checkpoint + durable queue |
| **No duplicates** | Idempotent processing (same input → same output) |
| **Progress monotonicity** | Status can only move forward: PENDING→RUNNING→COMPLETED |
| **Crash recovery** | RUNNING items older than timeout → re-queue |
| **Consistency** | Checkpoint write + output write in same transaction (or use output existence as checkpoint) |

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Isn't checkpointing every file slow?" | "Each checkpoint is a single row upsert — typically < 1ms. For a 5-hour migration with 50K files, that's ~50 seconds of total checkpoint I/O spread across the entire job. We can also batch checkpoint writes if needed." |
| "What if the checkpoint store itself fails?" | "The checkpoint store (Postgres) is running with replicas and WAL. If it's temporarily unavailable, workers pause and retry. We never proceed without confirming the checkpoint write." |
| "What about consistency between checkpoint and output?" | "Two approaches: (1) Write output to S3 first, then update checkpoint — on resume, we check if output exists before re-processing. (2) Use the output's existence as the checkpoint itself — no separate checkpoint store needed for simple cases." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **per-file checkpointing system** where each item in the migration tracks its state (PENDING→RUNNING→COMPLETED/FAILED) in a checkpoint store (Postgres). Workers write a checkpoint before starting and after completing each file. On resume, the system queries for non-COMPLETED items and re-queues them. Stale RUNNING items (detected by heartbeat timeout) are also re-queued. All processing is idempotent, so re-runs produce the same output. For very large files, I'd add sub-checkpoints at each processing stage. This guarantees that a 5-hour migration failing at 99% resumes from the last incomplete file — not from scratch."

---

## 9. Key Terms to Drop Naturally

- **Checkpoint**, **write-ahead log (WAL)**
- **Idempotent processing**
- **Heartbeat**, **liveness detection**
- **State machine** (PENDING→RUNNING→COMPLETED)
- **Resume semantics**, **crash recovery**
- **Event sourcing** (as an alternative approach)
