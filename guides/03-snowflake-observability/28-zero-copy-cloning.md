# Design a Zero-Copy Cloning Feature

> **Interview Prompt:** "How would you let users clone a 10TB database instantly without duplicating any data?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How large are the tables/databases? | Justifies why physical copy is impractical |
| 2 | Are clones read-only or read-write? | Copy-on-write complexity |
| 3 | How long do clones live? (minutes, months?) | Storage impact of divergence |
| 4 | How many concurrent clones? | Metadata overhead |
| 5 | Do we need cloning at table, schema, or database level? | Granularity of metadata |

---

## 2. How Zero-Copy Cloning Works

```
Original Table: "production.orders" (10TB)
  Stored as micro-partitions on S3:
    [P1] [P2] [P3] [P4] ... [P10000]

Clone command: CREATE TABLE dev.orders CLONE production.orders

What happens:
  ❌ Does NOT copy 10TB of data
  ✅ Creates new metadata pointing to the SAME partitions

Original metadata:
  production.orders → [P1, P2, P3, ..., P10000]

Clone metadata:
  dev.orders → [P1, P2, P3, ..., P10000]  ← same pointers!

Time to clone 10TB: ~1 second (metadata-only operation)
Additional storage: ~0 bytes
```

```
Why is this possible?

Key insight: Micro-partitions are IMMUTABLE.
  - An UPDATE doesn't modify P1 in place
  - It creates a NEW partition (P1') with the changes
  - The old P1 still exists, unchanged, on S3
  
So sharing partitions between original and clone is safe:
  - Neither can accidentally corrupt the other's data
  - Each table has its own metadata (list of partition pointers)
  - Modifications create new partitions, leaving shared ones untouched
```

---

## 3. Copy-on-Write (When Clones Diverge)

```
After cloning, user modifies the clone:

UPDATE dev.orders SET status = 'archived' WHERE date < '2023-01-01';

This affects partitions P1-P500. What happens:

BEFORE UPDATE:
  production.orders → [P1, P2, ..., P500, P501, ..., P10000]
  dev.orders        → [P1, P2, ..., P500, P501, ..., P10000]
                       ↑ shared                ↑ shared

AFTER UPDATE (copy-on-write):
  production.orders → [P1, P2, ..., P500, P501, ..., P10000]  (unchanged)
  dev.orders        → [P1', P2', ..., P500', P501, ..., P10000]
                       ↑ new (modified)        ↑ still shared

Only the modified partitions are new. 
The other 9,500 partitions remain shared → zero-copy.

Storage overhead: 500 new partitions only
  (not 10,000 — only 5% of the data was duplicated)
```

### Subsequent Modifications
```
Each modification only creates new partitions for the affected rows:

UPDATE dev.orders SET region = 'EU' WHERE id = 42;
  → Only partition containing id=42 is rewritten (1 partition)
  → dev.orders now has 501 unique partitions + 9,499 shared

DELETE FROM dev.orders WHERE date < '2022-01-01';
  → Affected partitions are removed from dev's metadata
  → Removed partitions still exist for production (shared, not deleted)
```

---

## 4. Architecture

```
┌──────────────────────────────────────────────┐
│            Metadata Service                   │
│                                               │
│  Table: production.orders                     │
│    Version: 42                                │
│    Partitions: [P1, P2, ..., P10000]         │
│    Active partition count: 10,000             │
│                                               │
│  Table: dev.orders (CLONE of production@v42) │
│    Version: 3                                 │
│    Base: production.orders@v42               │
│    Overrides: [P1→P1', P2→P2', ..., P500→P500']│
│    Active partition count: 10,000             │
│    Unique partitions: 500                     │
│    Shared partitions: 9,500                   │
└──────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────┐
│    Storage Layer (S3 — immutable partitions)  │
│                                               │
│  [P1] ← refcount: 1 (only production)       │
│  [P1']← refcount: 1 (only dev)              │
│  [P501]← refcount: 2 (both tables)          │
│  ...                                         │
│                                               │
│  Garbage collect when refcount = 0            │
└──────────────────────────────────────────────┘
```

### Reference Counting
```
P501 refcount lifecycle:
  1. Created by production.orders → refcount = 1
  2. Cloned by dev.orders → refcount = 2
  3. dev.orders dropped → refcount = 1 (not GC'd!)
  4. production.orders UPDATE rewrites P501 → refcount = 0 → GC

GC only happens when NO table references a partition.
```

---

## 5. Time Travel + Cloning

```
Snowflake combines zero-copy cloning with time travel:

CREATE TABLE debug.orders CLONE production.orders AT (
    TIMESTAMP => '2024-02-09 10:00:00'::TIMESTAMP
);

This creates a clone of the table AS IT WAS at that timestamp.
  - Metadata points to the partitions that were active at that time
  - Historical partitions are retained for the time travel window
  - Clone gets its own independent copy of that historical state

Use case: "Something went wrong yesterday at 10am. 
           Clone the table from that point so I can investigate 
           without affecting production."
```

---

## 6. Use Cases

| Use Case | How It Works |
|----------|-------------|
| **Dev/test environments** | Clone production instantly. Dev has real data without copying TBs |
| **What-if analysis** | Clone → run transforms → compare results → drop clone |
| **Safe schema migration** | Clone → apply ALTER TABLE → validate → swap with production |
| **Incident investigation** | Clone from time-travel point → query freely without production impact |
| **A/B testing data** | Two clones with different transforms → compare outcomes |
| **Cross-team sharing** | Each team clones shared dataset → modifies independently |

---

## 7. Comparison to Alternatives

| Approach | Time for 10TB | Extra Storage | Risk |
|----------|--------------|---------------|------|
| **Physical copy** (`CREATE TABLE AS SELECT`) | Hours | 10TB | High (disk space, I/O) |
| **View** (`CREATE VIEW`) | Instant | 0 | No write independence |
| **Materialized view** | Hours | 10TB | Stale data |
| **Zero-copy clone** | ~1 second | ~0 bytes | None ✅ |

---

## 8. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Clone divergence** (many writes) | Storage grows proportional to changes only, not total size |
| **Many clones of same table** | Metadata overhead, but storage shared. Limit clone count per table |
| **Partition GC complexity** | Background GC process with reference counting per partition |
| **Time-travel retention cost** | Configurable retention (1-90 days). Auto-purge old partitions |
| **Clone-of-clone** | Support recursive cloning. New clone shares whatever the parent shares |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if both original and clone are modified?" | "Each operates independently via copy-on-write. Modified partitions are unique to each table. Unmodified partitions remain shared. There's no merge conflict — they're independent tables that happen to share immutable storage." |
| "Doesn't this cause storage growth over time?" | "Only modified partitions use new storage. For a clone that changes 5% of data, you pay 5% extra storage, not 100%. Reference counting garbage-collects orphaned partitions when no table references them." |
| "How is this different from a view?" | "A view is a query alias — no independent data. A clone creates an independent table at the metadata level. You can DROP the original and the clone still works (it owns references to the shared partitions). You can also write to a clone, which you can't do with a view." |
| "Can you clone across regions?" | "Cross-region cloning requires replicating the underlying partitions to the target region (since S3 is region-scoped). This is full copy with data transfer, not zero-copy. Within the same region and storage account, it's instant." |

---

## 10. Summary: Your Interview Narrative

> "Zero-copy cloning is a **metadata-only operation** enabled by immutable storage. Data is stored as immutable micro-partitions on S3. Cloning creates new metadata pointing to the same partitions as the original — no data is copied. Writes use **copy-on-write**: only modified partitions create new physical data; unmodified partitions remain shared. Reference counting tracks how many tables point to each partition, enabling safe garbage collection. Combined with **time travel**, you can clone a 10TB table from any point in the past in under a second with zero additional storage."

---

## 11. Key Terms to Drop Naturally

- **Copy-on-write**, **metadata-only operation**
- **Micro-partition**, **immutable storage**
- **Reference counting**, **garbage collection**
- **Snapshot isolation**, **time travel**
- **Zero-copy**, **storage sharing**
- **Partition pruning** (reading from clones is equally fast)
