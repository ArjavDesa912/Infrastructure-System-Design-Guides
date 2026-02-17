# Change Data Capture Pipeline

> **Interview Prompt:** "A Fortune 500 company is migrating their Oracle production database to Snowflake. They can't afford downtime. Design a zero-downtime CDC pipeline that keeps Snowflake in sync during migration."

---

## 1. Requirements

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

## 2. API Design

### Pipeline Configuration
```
POST /v1/cdc-pipelines
{
  "source": {
    "type": "oracle",
    "connection": "oracle://cdc_user@prod-oracle:1521/ORCL",
    "schemas": ["HR", "FINANCE", "ORDERS"],
    "capture_mode": "log_based"
  },
  "sink": {
    "type": "snowflake",
    "account": "acme.us-east-1",
    "database": "MIGRATION_TARGET",
    "warehouse": "CDC_WH_MEDIUM"
  },
  "config": {
    "initial_snapshot": true,
    "batch_interval_seconds": 10,
    "on_schema_change": "evolve"
  }
}
```

### Pipeline Status
```
GET /v1/cdc-pipelines/{id}/status

{
  "state": "STREAMING",
  "replication_lag_seconds": 4.2,
  "throughput_events_per_second": 12400,
  "source_position": "scn=982347123",
  "tables_synced": 497
}
```

---

## 3. High-Level Architecture

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

## 4. Deep Dive: CDC Capture Strategies

### 4.1 Log-Based CDC (Recommended)

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

**Why log-based?** At 5,000 TPS, triggers double write load on production. Log-based reads the existing WAL asynchronously with zero impact.

### 4.2 Exactly-Once Apply via MERGE

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

MERGE is inherently idempotent — applying the same event twice produces the same result. This handles Kafka redelivery safely.

> *This MERGE-based idempotency mirrors VibeDB's merge-on-read approach for LSM trees — naturally deduplicating by taking the latest version of each key.*

### 4.3 Schema Evolution

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

## 5. Deep Dive: Initial Load + CDC Consistency

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

## 6. Deep Dive: Zero-Downtime Cutover

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

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Debezium crashes** | CDC stops | GKE restarts; resumes from last Kafka Connect offset |
| **Kafka broker failure** | Delivery paused | 3-broker cluster, replication factor 3 |
| **Snowflake warehouse suspended** | Sink stalls | Alert on lag; auto-resume on CDC activity |
| **Schema change breaks MERGE** | Type mismatch errors | Schema evolution handler + pause mode |
| **Oracle redo log rotation** | CDC falls behind | Configure archive log retention > lag |
| **Network partition** | Capture stalls | Debezium auto-reconnects; heartbeat detects staleness |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Debezium | Oracle GoldenGate | Open-source, Kafka-native, no licensing |
| Kafka as bus | Direct to Snowflake | Durability, replay, decoupling, multiple consumers |
| MERGE INTO | Row-by-row ops | Idempotent, batch-efficient, handles all op types |
| Micro-batch (10s) | Snowpipe Streaming (1s) | 10s sufficient for migration; 5x cheaper on compute |
| SCN-based snapshot | Lock-based | Non-blocking; locks halt production |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not big-bang weekend migration?" | "For Fortune 500, 4 hours downtime costs millions. CDC allows continuous replication with a 30-second cutover. Infrastructure complexity is justified by business risk reduction." |
| "What if CDC lag grows?" | "Alert at 30s, page at 2min. Root causes: undersized warehouse (scale up), Kafka consumer lag (add partitions), network saturation (compress). Dashboard per-table lag isolates bottleneck." |
| "Data types that don't map?" | "Type mapping registry: Oracle NUMBER(38)→Snowflake NUMBER(38), DATE→TIMESTAMP_NTZ, CLOB→VARCHAR(16MB). Use widest compatible type for ambiguous mappings." |
| "Data corruption after cutover?" | "72-hour rollback window with Oracle in read-only + reverse CDC. Revert DNS in seconds. Row checksums isolate corrupt data." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **log-based CDC pipeline** using **Debezium on GKE** reading Oracle redo logs, streaming through **Kafka** (topic per table, partitioned by PK), applying to Snowflake via **MERGE INTO** in 10-second micro-batches. Initial bulk load uses a consistent SCN snapshot → Parquet → GCS → COPY INTO. CDC starts from snapshot SCN — no gap, no overlap. Schema evolution auto-applies compatible changes. Cutover is a 30-second quiesce → drain → SCN match verification → DNS switch, with 72-hour rollback window."

---

## 11. Key Terms to Drop Naturally

- **CDC**, **log-based capture**, **Debezium**
- **SCN (System Change Number)**, **redo log**, **WAL**
- **MERGE INTO**, **idempotent upsert**
- **Consistent snapshot**, **AS OF SCN**
- **Micro-batch**, **Snowpipe Streaming**
- **Zero-downtime cutover**, **quiesce window**
- **Replication lag**, **consumer lag**
- **Schema evolution**, **type mapping registry**
