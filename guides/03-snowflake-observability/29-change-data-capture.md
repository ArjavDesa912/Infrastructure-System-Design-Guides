# Design a Change Data Capture (CDC) Pipeline

> **Interview Prompt:** "Design a system that captures every change in one database and streams it to another system in real-time."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Source database? (Postgres, MySQL, SQL Server?) | CDC mechanism differs by DB |
| 2 | What downstream systems? (warehouse, cache, search index?) | Consumer design |
| 3 | Ordering requirements? (per-row, per-table, global?) | Kafka partitioning |
| 4 | Latency tolerance? (seconds, minutes?) | Batch vs. streaming |
| 5 | Do we need the initial full load, or only future changes? | Snapshot + streaming design |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────┐
│                Source Database (Postgres)                  │
│                                                           │
│  Application writes → WAL (Write-Ahead Log)              │
│                                                           │
│  INSERT INTO orders VALUES (1, 'pending', 100.00)        │
│    → WAL entry: {txn:42, table:orders, op:INSERT, ...}   │
└──────────────────────────┬───────────────────────────────┘
                           │ Logical replication slot
                           ▼
┌──────────────────────────────────────────────────────────┐
│               CDC Connector (Debezium)                    │
│                                                           │
│  1. Reads WAL continuously                                │
│  2. Converts to structured change events                  │
│  3. Publishes to Kafka                                    │
│  4. Tracks position (LSN) for resume                     │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                    Kafka Topics                           │
│                                                           │
│  cdc.public.orders     → [partition 0] [partition 1] ... │
│  cdc.public.customers  → [partition 0] [partition 1] ... │
│  cdc.public.products   → [partition 0] [partition 1] ... │
└────────┬─────────────┬─────────────┬─────────────────────┘
         │             │             │
         ▼             ▼             ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │Snowflake │  │Elastic-  │  │Redis     │
  │Sink      │  │search    │  │Cache     │
  │(batch    │  │Sink      │  │Invalidat │
  │ MERGE)   │  │(upsert)  │  │or        │
  └──────────┘  └──────────┘  └──────────┘
```

---

## 3. Deep-Dive: CDC Mechanisms

### 3.1 Log-Based CDC (Recommended)

```
How the Write-Ahead Log works:

Every database write goes to the WAL BEFORE the table:
  1. App: INSERT INTO orders (id, status) VALUES (1, 'new')
  2. DB writes to WAL: 
     { lsn: "0/1A2B3C", txn_id: 42, 
       table: "orders", op: "INSERT",
       data: {id: 1, status: "new"} }
  3. DB writes to table (actual data pages)

Debezium reads the WAL via logical replication:
  - Non-invasive: no triggers, no polling, no app changes
  - Captures ALL changes, including DELETEs
  - Exact ordering by LSN (Log Sequence Number)
  - Minimal impact on source DB (reading log, not querying tables)
```

### 3.2 Comparison of CDC Methods

| Method | How It Works | Pros | Cons |
|--------|-------------|------|------|
| **Log-based (WAL)** | Read database write-ahead log | No source impact, captures all ops, ordering | Requires DB configuration (replication slot) |
| **Trigger-based** | DB triggers write changes to shadow table | Works on any DB | Adds write overhead to every operation |
| **Polling (timestamp)** | `SELECT * WHERE updated_at > last_poll` | Simple, no DB config | Misses DELETEs, high latency, polling load |
| **Query-based (diff)** | Compare full table snapshots periodically | Works on any DB | Very expensive for large tables |

### 3.3 Change Event Format

```json
{
    "schema": { ... },
    "payload": {
        "op": "u",
        "before": {
            "id": 1,
            "status": "pending",
            "amount": 100.00
        },
        "after": {
            "id": 1,
            "status": "shipped",
            "amount": 100.00
        },
        "source": {
            "version": "2.4.0",
            "connector": "postgresql",
            "db": "mydb",
            "schema": "public",
            "table": "orders",
            "lsn": "0/1A2B3C",
            "txid": 42,
            "ts_ms": 1707500000000
        },
        "ts_ms": 1707500000123
    }
}

Operation types:
  "c" = CREATE (INSERT)
  "u" = UPDATE
  "d" = DELETE (before has data, after is null)
  "r" = READ (snapshot/initial load)
```

---

## 4. Kafka Topic Design

```
Topic per table:
  cdc.public.orders
  cdc.public.customers
  cdc.public.products

Partition key: primary key (ensures ordering per row)
  → Row id=42 always goes to the same partition
  → All changes to row 42 arrive in strict order

Why per-row ordering matters:
  INSERT (id=1, status='new')
  UPDATE (id=1, status='shipped')    ← must come AFTER insert
  DELETE (id=1)                      ← must come AFTER update
  
  If out-of-order → consumer might delete before insert → data loss
  
  Kafka guarantees ordering WITHIN a partition ✅
  Partition key = primary key → same row = same partition ✅
```

---

## 5. Consumer Patterns

### 5.1 Snowflake (Batch MERGE)

```sql
-- Accumulate changes over 5-minute micro-batches
-- Then MERGE into target table

MERGE INTO analytics.orders AS target
USING staging.order_changes AS source
ON target.id = source.id
WHEN MATCHED AND source.op = 'd' THEN DELETE
WHEN MATCHED AND source.op = 'u' THEN UPDATE SET
    status = source.after_status,
    amount = source.after_amount
WHEN NOT MATCHED AND source.op IN ('c', 'r') THEN INSERT
    (id, status, amount) VALUES 
    (source.after_id, source.after_status, source.after_amount);
```

### 5.2 Elasticsearch (Real-Time Upsert)

```
For each change event:
  op = "c" or "u": 
    PUT /orders/_doc/{id} → index/update document
  op = "d":
    DELETE /orders/_doc/{id} → remove document
```

### 5.3 Redis (Cache Invalidation)

```
For each change event:
  DEL cache:order:{id}
  
  (Don't update the cache — just invalidate it.
   Next read will fetch fresh data from the source of truth.)
```

---

## 6. Initial Load (Snapshot)

```
Problem: When first setting up CDC, the WAL only has recent data.
         We need the existing table contents too.

Debezium snapshot process:
  1. Lock table briefly (consistent snapshot point)
  2. Read all existing rows → publish as "r" (read) events
  3. Record the LSN at snapshot time
  4. Release lock
  5. Switch to streaming mode from the recorded LSN

Consumer sees:
  [1000 "r" events — existing data] → [streaming "c/u/d" events — new changes]
  
  No gap between snapshot and streaming ✅
  No duplicates (LSN ensures exact cutover point) ✅
```

---

## 7. Exactly-Once Delivery

```
Challenge: What if the consumer crashes after processing but before committing offset?

At-least-once + idempotent consumers:
  1. Consumer reads event: UPDATE order #42
  2. Applies to Snowflake
  3. Commits Kafka offset
  → If crash between step 2 and 3: event reprocessed
  → But MERGE is idempotent (same update applied twice = same result) ✅

Alternative: Transactional outbox in consumer
  1. Read Kafka event
  2. In single DB transaction: apply change + save Kafka offset
  3. If crash: transaction rolls back, both change and offset lost
  → Re-process from last committed offset ✅
```

---

## 8. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **WAL growth on source** | Monitor replication lag; increase WAL retention; tune Debezium batch size |
| **High-volume tables** | Increase Kafka partitions; parallelize consumers |
| **Schema changes on source** | Debezium emits DDL change events; consumers handle via schema registry |
| **Consumer lag** | Auto-scale consumers; use consumer groups with partition assignment |
| **Initial snapshot takes too long** | Parallel snapshot (chunk by primary key); incremental snapshots |

---

## 9. Security Considerations

```
Data Privacy:
  - PII filtering: mask or redact sensitive columns before publishing
  - Field-level encryption: encrypt credit cards, SSNs at source
  - GDPR compliance: right-to-be-forgotten via delete events

Access Control:
  - Source database: CDC connector uses least-privilege account (REPLICATION permission only)
  - Kafka topics: ACLs restrict who can consume CDC events
  - Destination systems: separate credentials per consumer

Audit & Compliance:
  - Immutable log: Kafka retain-all for change history
  - Chain of custody: track every data change from source to destination
  - Compliance reports: generate "what changed when" reports for auditors
```

---

## 9. Testing Change Data Capture

```python
# Test: End-to-end CDC flow
def test_cdc_pipeline():
    # Insert row in source
    source_db.execute("INSERT INTO orders (id, amount) VALUES (1, 100)")

    # Wait for CDC event
    event = consume_cdc_event(timeout=5)
    assert event["op"] == "c"  # create
    assert event["after"]["id"] == 1
    assert event["after"]["amount"] == 100

    # Verify in destination
    result = dest_db.execute("SELECT * FROM orders WHERE id = 1")
    assert result[0]["amount"] == 100

# Test: Exactly-once semantics
def test_exactly_once():
    # Send CDC event
    event = {"op": "u", "before": {"id": 1, "amount": 100}, "after": {"id": 1, "amount": 200}}

    # Process twice (simulating retry)
    process_event(event)
    process_event(event)

    # Verify result is same (idempotent)
    result = dest_db.execute("SELECT amount FROM orders WHERE id = 1")
    assert result[0]["amount"] == 200  # Not 300 or 400

# Test: Schema evolution handling
def test_schema_evolution():
    # Add new column to source
    source_db.execute("ALTER TABLE orders ADD COLUMN discount INT")

    # Insert row with new column
    source_db.execute("INSERT INTO orders (id, amount, discount) VALUES (2, 100, 10)")

    # CDC should handle schema change
    event = consume_cdc_event(timeout=5)
    assert event["after"]["discount"] == 10
```

---

## 10. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What about schema changes on the source?" | "Debezium detects DDL changes and publishes schema change events. We use a schema registry with backward compatibility — consumers can read old and new formats. For breaking changes, we version topics." |
| "What if Kafka is down?" | "Debezium pauses and retains its LSN position. The source DB WAL continues accumulating changes. When Kafka recovers, Debezium resumes from its saved position — no data loss. We size WAL retention to cover expected downtime." |
| "How do you handle deletes?" | "Log-based CDC captures DELETE operations natively. The change event has `op: 'd'` with the `before` payload containing the deleted row's data. Consumers apply the delete downstream (MERGE...DELETE, or Elasticsearch DELETE)." |
| "What about multi-table transactions?" | "Debezium can emit transaction boundary events (BEGIN/COMMIT). Consumers can buffer changes within a transaction and apply them atomically. Most consumers don't need this — eventual consistency per-row is sufficient." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **log-based CDC pipeline** using Debezium to read the source database's WAL via logical replication. Change events (INSERT, UPDATE, DELETE) are published to per-table Kafka topics, partitioned by primary key for per-row ordering. Consumers apply changes downstream: Snowflake via batch MERGE every 5 minutes, Elasticsearch via real-time upsert, Redis via cache invalidation. Debezium handles initial load with a consistent snapshot, then seamlessly switches to streaming. At-least-once delivery with idempotent consumers gives us effective exactly-once semantics."

---

## 11. Key Terms to Drop Naturally

- **Write-Ahead Log (WAL)**, **logical replication**
- **Debezium**, **CDC connector**
- **LSN** (Log Sequence Number) / **binlog** position
- **Initial snapshot**, **snapshot + streaming**
- **Exactly-once semantics**, **idempotent consumer**
- **MERGE statement** (upsert pattern for batch consumers)
- **Schema evolution**, **schema registry**
