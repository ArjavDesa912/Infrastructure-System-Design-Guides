# Design a Garbage Collector

> **Interview Prompt:** "Design a system for cleaning up old temporary files after a migration completes."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What types of resources need cleanup? (files, DB rows, cache entries?) | Scope of the collector |
| 2 | How do we know something is "garbage"? (TTL, reference counting, ownership?) | Collection algorithm |
| 3 | What's the cost of collecting too early? (data loss!) | Safety margin |
| 4 | What's the cost of collecting too late? (storage costs, quota) | Collection frequency |
| 5 | Are there compliance requirements? (legal hold, audit trail?) | Retention policies |

---

## 2. High-Level Architecture

```
┌───────────────────────────────────────────────────┐
│                  Resource Registry                  │
│  ┌─────────────────────────────────────────────┐  │
│  │ Resource     │ Owner    │ Created  │ State    │  │
│  │ /tmp/job-1/  │ job-1    │ 2h ago   │ ORPHANED │  │
│  │ /tmp/job-2/  │ job-2    │ 5m ago   │ IN_USE   │  │
│  │ /tmp/job-3/  │ job-3    │ 3d ago   │ COMPLETED│  │
│  └─────────────────────────────────────────────┘  │
└───────────────────────┬───────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────┐
│              Garbage Collector Service              │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │  Mark     │  │  Sweep   │  │  Compact │         │
│  │  Phase    │──▶  Phase   │──▶  Phase   │         │
│  │          │  │          │  │(optional)│         │
│  └──────────┘  └──────────┘  └──────────┘         │
└───────────────────────────────────────────────────┘
```

---

## 3. Deep-Dive: GC Strategies

### 3.1 Mark-and-Sweep (Most Common)

```
Mark Phase:
  1. Start from "roots" (active jobs, live references)
  2. Traverse all reachable resources → mark as LIVE
  3. Everything not marked = garbage

Sweep Phase:
  1. Iterate through all resources
  2. If not marked LIVE → DELETE

Example:
  Active jobs: [job-2]
  
  Resources:
    /tmp/job-1/ → not referenced by any active job → GARBAGE ✅
    /tmp/job-2/ → referenced by job-2 → LIVE, keep
    /tmp/job-3/ → not referenced → GARBAGE ✅
    s3://cache/blob-A → referenced by job-2 → LIVE, keep
    s3://cache/blob-B → unreferenced → GARBAGE ✅
```

### 3.2 Reference Counting

```
Each resource tracks its reference count:
  blob-A: refcount = 2 (used by job-1 and job-3)
  blob-B: refcount = 1 (used by job-2)
  blob-C: refcount = 0 → GARBAGE, delete immediately

When:
  - Job creates/uses resource → increment refcount
  - Job completes/deletes → decrement refcount
  - Refcount hits 0 → eligible for collection
```

- ✅ Immediate cleanup, simple
- ❌ Circular references (A→B→A, both refcount=1 but both garbage)
- ❌ Counting overhead on every operation

### 3.3 TTL-Based (Simplest)

```
Rules:
  - Temp files older than 24 hours → DELETE
  - Completed job artifacts older than 7 days → ARCHIVE to cold storage
  - Cache entries older than 1 hour → EVICT
  - Logs older than 90 days → DELETE

Cron job runs every hour:
  DELETE FROM temp_files WHERE created_at < NOW() - INTERVAL '24 hours'
  DELETE FROM s3_objects WHERE type = 'temp' AND age > '24h'
```

- ✅ Dead simple, predictable
- ❌ May delete resources still in use (if job runs longer than TTL)
- Mitigation: heartbeat — active jobs refresh their resource timestamps

### 3.4 Generational Collection

```
Generation 0 (Nursery): Resources < 1 hour old
  → Collected every 10 minutes
  → Most temp files die here (short-lived)

Generation 1 (Middle): Resources 1-24 hours old
  → Collected every hour
  → Intermediate artifacts, most are garbage

Generation 2 (Old): Resources > 24 hours old
  → Collected daily
  → Few resources, mostly permanent
```

- ✅ Most garbage is short-lived → frequent nursery collection catches it
- ✅ Reduces overhead for long-lived resources

---

## 4. What to Collect

| Resource Type | Cleanup Strategy |
|--------------|-----------------|
| **Temp extraction files** | TTL (24h after job completes) |
| **Intermediate S3 objects** | Reference counting by job |
| **Orphaned queue messages** | Visibility timeout + DLQ |
| **Stale DB rows** (failed jobs) | Mark-and-sweep weekly |
| **Cache entries** | LRU eviction + TTL |
| **Log files** | TTL (90 day retention) |
| **API response cache** | TTL (5 min - 1 hour) |

### Cost Analysis

```
Assumptions:
  1M jobs/month, each creates ~20 temp files (avg 50KB each)
  S3 storage: $0.023/GB/month

Without GC:
  1M × 20 × 50KB = 1TB/month accumulated
  After 12 months: 12TB × $0.023/GB = $276/month (and growing!)

With GC (24h TTL for temp files):
  Active temp files at any time: ~33K jobs/day × 20 files × 50KB = 33GB
  Cost: $0.76/month (constant, not growing)

Savings: ~99.7% reduction in storage cost
Additional: S3 API costs for listing objects during sweep
  Mitigation: use S3 lifecycle policies for bulk TTL-based deletion
```

---

## 5. Safety: Avoiding Premature Deletion

| Risk | Mitigation |
|------|-----------|
| **Delete file still in use** | Grace period: mark as garbage, wait 24h, then delete |
| **Race condition** | Soft-delete first (mark deleted), hard-delete in next cycle |
| **Job runs longer than TTL** | Heartbeat: active jobs refresh timestamp on their resources |
| **Compliance: legal hold** | Check hold flags before deleting; never auto-delete held resources |

### Two-Phase Deletion Pattern

```
Phase 1 (Soft Delete):
  Resource marked as "PENDING_DELETION"
  Not accessible to new operations
  Still on disk

  Wait period: 24 hours

Phase 2 (Hard Delete):
  After grace period: physically delete
  Log deletion event for audit

Recovery:
  If someone discovers they need it during grace period:
  Restore from PENDING_DELETION → ACTIVE
```

---

## 6. Data Model

```sql
CREATE TABLE resource_registry (
    resource_id     UUID PRIMARY KEY,
    resource_type   ENUM('TEMP_FILE','S3_OBJECT','CACHE','DB_ROW'),
    resource_uri    TEXT,
    owner_job_id    UUID,
    state           ENUM('ACTIVE','ORPHANED','PENDING_DELETION','DELETED'),
    ref_count       INT DEFAULT 1,
    created_at      TIMESTAMP,
    last_accessed   TIMESTAMP,
    expires_at      TIMESTAMP,
    deleted_at      TIMESTAMP
);

CREATE INDEX idx_gc_candidates 
    ON resource_registry(state, expires_at) 
    WHERE state IN ('ORPHANED', 'PENDING_DELETION');
```

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if you delete something that's still needed?" | "Three safeguards: (1) Two-phase deletion with a 24h grace period before hard-delete, (2) Active jobs heartbeat to refresh their resources' timestamps, (3) Soft-deleted resources can be restored during the grace period." |
| "How do you handle cleanup at scale?" | "Batch deletion with rate limiting — we don't delete 1 million files at once, we process 1,000 per batch with a 1-second delay between batches. This prevents I/O storms and lets other operations proceed normally." |
| "What about the cost of tracking every resource?" | "We only track long-lived resources (S3 objects, large temp files). Ephemeral in-memory data uses TTL-based eviction in Redis. The registry itself is small — millions of rows in a single Postgres table." |
| "Why not just use S3 lifecycle policies?" | "S3 lifecycle policies are great for TTL-based bulk cleanup — and I'd use them as the base layer. But they can't handle reference counting (keep file if another job still needs it) or conditional cleanup (don't delete if there's a legal hold). The GC service adds intelligence on top of S3 lifecycle." |
| "How do you handle garbage that spans multiple storage systems?" | "The resource registry is storage-agnostic. It tracks the URI (s3://..., redis://..., postgres://...) and the cleanup logic dispatches to the appropriate storage client. This way, one GC sweep can clean S3 objects, Redis keys, and DB rows in a single pass." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **multi-strategy garbage collector** for migration cleanup. Temporary files use TTL-based collection (24h after job completion). S3 artifacts use reference counting — when a job completes, it decrements references, and zero-ref resources are collected. For safety, all deletion is two-phase: mark as PENDING_DELETION, wait a grace period, then hard-delete. Active jobs heartbeat to prevent premature cleanup. Collection runs at different frequencies: short-lived temp files are checked every 10 minutes, long-lived artifacts daily. All deletions are logged for audit."

---

## 9. Key Terms to Drop Naturally

- **Mark-and-sweep**, **reference counting**
- **Generational GC** (young/old generation)
- **Soft delete** / **two-phase deletion**
- **Grace period**, **tombstone**
- **TTL-based eviction**
- **Rate-limited batch deletion**
- **S3 lifecycle policy**
- **Storage-agnostic cleanup**
- **Legal hold**, **compliance retention**
