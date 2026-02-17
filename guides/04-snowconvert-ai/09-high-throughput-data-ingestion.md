# High-Throughput Data Ingestion

> **Interview Prompt:** "SnowConvert needs to ingest terabytes of legacy database exports — CSV dumps, Parquet files, Oracle Data Pump exports — into Snowflake as fast as possible. Design a high-throughput data ingestion pipeline optimized for performance."

---

## 1. Requirements

### Functional
- Ingest data from multiple source formats: CSV, Parquet, Avro, Oracle DMP, SQL Server BCP.
- Load into Snowflake tables with schema mapping and type conversion.
- Support both initial bulk loads (TBs) and incremental daily loads (GBs).
- Provide file-level tracking (which files were loaded, skipped, or failed).
- Support data validation: row counts, checksums, and sample verification.

### Non-Functional
- **Throughput:** Ingest 1 TB/hour for bulk loads (sustained).
- **Latency:** Incremental loads available in Snowflake within 60 seconds.
- **Reliability:** Zero data loss — every file is either loaded or error-reported.
- **Cost efficiency:** Minimize Snowflake credit consumption.

### Capacity Estimation
```
Bulk load:
  Data volume:         10 TB
  Target time:         10 hours → 1 TB/hour → 278 MB/sec
  File count:          100K files (avg 100MB each)
  Snowflake warehouse: XL (16 nodes) → ~250 MB/sec sustained write

Incremental load:
  Daily volume:        50 GB (500 files, avg 100 MB)
  Target time:         < 15 minutes → 56 MB/sec
  Snowflake warehouse: Medium (4 nodes) → sufficient

Network:
  GCS → Snowflake:     Internal Google Cloud network (10+ Gbps)
  External upload:     Customer network → GCS signed URL
  
Cost:
  XL warehouse (10hrs): 10 × 16 credits/hr × $2/credit = $320
  Medium warehouse (15min/day): 0.25 × 4 × $2 × 30 = $60/month
  GCS storage (10 TB): $200/month
```

---

## 2. API Design

### Submit Ingestion Job
```
POST /v1/ingestion-jobs
{
  "source": {
    "type": "gcs",
    "prefix": "gs://staging/tenant-42/export-2025-01-15/",
    "format": "parquet",
    "compression": "snappy"
  },
  "target": {
    "database": "MIGRATION_TARGET",
    "schema": "HR",
    "table": "EMPLOYEES",
    "warehouse": "INGEST_XL",
    "load_mode": "append"          // "append" | "truncate_load" | "merge"
  },
  "config": {
    "parallelism": 16,
    "on_error": "continue",        // "continue" | "abort_statement" | "skip_file"
    "validation": "row_count"       // "row_count" | "checksum" | "none"
  }
}
```

---

## 3. High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                External Data Sources                           │
│  Oracle Data Pump │ SQL Server BCP │ CSV/Parquet exports       │
└─────────┬─────────┴───────┬────────┴──────────┬───────────────┘
          │                 │                    │
          ▼                 ▼                    ▼
┌───────────────────────────────────────────────────────────────┐
│              Format Normalizer (GKE Pods)                      │
│  - Converts Oracle DMP → Parquet                               │
│  - Converts BCP → Parquet                                      │
│  - CSV → Parquet (with schema inference)                       │
│  - Splits large files into optimal chunks (256MB)              │
│  - Output: Parquet files in GCS staging area                   │
└─────────────────────────┬─────────────────────────────────────┘
                          │
┌─────────────────────────▼─────────────────────────────────────┐
│              GCS Staging Area                                   │
│  gs://staging/tenant-42/normalized/                             │
│  ├── employees_chunk_001.parquet  (256 MB)                      │
│  ├── employees_chunk_002.parquet  (256 MB)                      │
│  ├── employees_chunk_003.parquet  (256 MB)                      │
│  └── ...                                                        │
└─────────────────────────┬─────────────────────────────────────┘
                          │
┌─────────────────────────▼─────────────────────────────────────┐
│              Snowflake Loader                                   │
│                                                                 │
│  Option A: COPY INTO (Batch, high throughput)                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  COPY INTO HR.EMPLOYEES                                  │   │
│  │  FROM @staging_stage/normalized/                          │   │
│  │  FILE_FORMAT = (TYPE = PARQUET)                          │   │
│  │  PATTERN = 'employees_chunk_.*\.parquet'                 │   │
│  │  ON_ERROR = CONTINUE;                                    │   │
│  │                                                          │   │
│  │  Snowflake parallelizes across warehouse nodes           │   │
│  │  16-node XL: 16 files loaded simultaneously              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Option B: Snowpipe (Continuous, event-driven)                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  GCS notification → Pub/Sub → Snowpipe auto-ingest      │   │
│  │  - Auto-loads files as they arrive in GCS                │   │
│  │  - No warehouse needed (serverless compute)              │   │
│  │  - Latency: 30-60 seconds                                │   │
│  │  - Cost: $0.06 per 1,000 files                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Option C: Snowpipe Streaming (Real-time, lowest latency)      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  SDK-based row-level ingestion                           │   │
│  │  - Sub-second latency                                    │   │
│  │  - Ideal for CDC events, not bulk                        │   │
│  └─────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive: Performance Optimization

### 4.1 File Sizing (The Pareto Principle of Ingestion)

```
File size vs. COPY INTO performance:

  Too small (< 10 MB):
    - Overhead per file dominates (metadata, scheduling)
    - 100K tiny files → 100K scheduling operations
    - Throughput: ~50 MB/sec ❌

  Too large (> 1 GB):
    - One file = one thread in Snowflake
    - Can't parallelize within a single file
    - Uneven distribution across warehouse nodes
    - Throughput: ~100 MB/sec ❌

  Optimal (100-250 MB compressed):
    - Low per-file overhead
    - Many files = many threads = full parallelism
    - Even distribution across nodes
    - Throughput: 250+ MB/sec ✅

Rule of thumb: Number of files ≥ number of warehouse nodes × 4
  XL warehouse (16 nodes): need ≥ 64 files for full utilization
```

### 4.2 Format Selection

| Format | Compression | Read Speed | Snowflake Support | When to Use |
|--------|-------------|-----------|-------------------|-------------|
| **Parquet** | Snappy (3-5x) | Columnar, fast | Native COPY INTO | Default choice for bulk |
| **CSV** | Gzip (5-10x) | Row-based, slower | Native COPY INTO | Legacy sources, no Parquet option |
| **Avro** | Deflate | Row-based | Native COPY INTO | Kafka CDC events |
| **ORC** | Zlib | Columnar | Native COPY INTO | Hive/Hadoop exports |
| **JSON** | Gzip | Semi-structured | VARIANT column | Nested/dynamic schemas |

**Why Parquet?**
- Columnar → Snowflake only reads needed columns during COPY
- Built-in schema → no schema inference needed
- Snappy compression → fast decompression with good ratio
- Type-safe → fewer type conversion errors

### 4.3 Parallel Loading Strategy

```
10 TB table, XL warehouse (16 nodes):

  Step 1: Split into 256 MB chunks → 40,000 files
  Step 2: Stage to GCS with distributed prefixes:
    gs://staging/tenant-42/chunk_group_00/  (2,500 files)
    gs://staging/tenant-42/chunk_group_01/  (2,500 files)
    ...
    gs://staging/tenant-42/chunk_group_15/  (2,500 files)
  
  Step 3: Execute parallel COPY INTO statements:
    -- Snowflake distributes files across 16 nodes automatically
    COPY INTO HR.EMPLOYEES
    FROM @staging_stage
    FILE_FORMAT = (TYPE = PARQUET)
    ON_ERROR = CONTINUE;
    
    -- Each node loads ~2,500 files
    -- 16 nodes × ~15 MB/sec each = ~250 MB/sec aggregate
    -- 10 TB / 250 MB/sec = ~11 hours

  Optimization: Use MATCH_BY_COLUMN_NAME
    COPY INTO HR.EMPLOYEES
    FROM @staging_stage
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    -- Handles column ordering differences automatically
```

### 4.4 GCS Read Optimization

```
GCS Performance Tips:
  1. Distribute files across prefixes (avoid single-prefix hotspot)
     Bad:  gs://data/all_files/chunk_0001.parquet  (all in one prefix)
     Good: gs://data/00/chunk_0001.parquet
           gs://data/01/chunk_0002.parquet
     
  2. Use regional GCS bucket co-located with Snowflake account
     Same region: ~1 Gbps per stream
     Cross-region: ~200 Mbps per stream (5x slower + egress cost)
  
  3. Enable parallel downloads in Snowflake
     ALTER SESSION SET USE_CACHED_RESULT = FALSE;
     -- Forces fresh reads (useful for re-loads)
```

---

## 5. Deep Dive: Type Mapping and Conversion

```
Oracle → Snowflake Type Mapping:

  Oracle                    Snowflake               Notes
  ─────────────────────────────────────────────────────────
  NUMBER(38)               NUMBER(38,0)             Exact match
  NUMBER(10,2)             NUMBER(10,2)             Exact match
  VARCHAR2(4000)           VARCHAR(4000)            Simple rename
  CLOB                     VARCHAR(16777216)        16MB max in SF
  BLOB                     BINARY                   Or store in stage
  DATE                     TIMESTAMP_NTZ            Oracle DATE has time
  TIMESTAMP WITH TZ        TIMESTAMP_TZ             Direct mapping
  RAW(16)                  BINARY(16)               UUID common case
  XMLTYPE                  VARIANT                  Parse XML to JSON
  SDO_GEOMETRY             GEOGRAPHY                Spatial data
  
  Problematic types:
  - LONG:     Deprecated in Oracle, no direct mapping → VARCHAR(16MB)
  - BFILE:    External file reference → must extract content to stage
  - USER_TYPE: Custom types → flatten to columns
```

> *This type mapping registry is analogous to the dialect conversion rules in the SQL migration agent I built — each source type maps to the widest compatible Snowflake type, with explicit documentation of precision loss risks.*

---

## 6. Deep Dive: Data Validation

```
Three-Phase Validation:

Phase 1: Pre-Load (in GCS staging)
  - File integrity: checksum verification for each Parquet file
  - Schema validation: column names, types match target table
  - Data sampling: read 1% of rows, check for obvious corruption
  
Phase 2: Post-Load (in Snowflake)
  SELECT 
    (SELECT COUNT(*) FROM source_gcs_table) as source_rows,
    (SELECT COUNT(*) FROM target_sf_table) as target_rows,
    source_rows = target_rows as row_count_match;
  
  -- Checksum comparison (approximate)
  SELECT HASH_AGG(*) FROM target_sf_table;
  -- Compare with pre-computed source hash

Phase 3: Ongoing (business logic)
  -- Known invariants
  SELECT COUNT(*) FROM HR.EMPLOYEES WHERE salary < 0;  -- Should be 0
  SELECT COUNT(*) FROM ORDERS WHERE amount != quantity * unit_price;
```

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Corrupt Parquet file** | COPY INTO skips or fails | `ON_ERROR = CONTINUE` + error log; re-export corrupt file |
| **Type conversion failure** | Rows rejected | `VALIDATION_MODE = RETURN_ERRORS` → fix mapping → retry |
| **Warehouse timeout** | Long COPY INTO killed | Split into smaller COPY batches (1000 files each) |
| **GCS throttled** | Slow reads | Distribute prefixes; exponential backoff |
| **Snowflake credit limit** | Load paused by admin | Monitor credit usage; alert at 80% budget |
| **Schema mismatch** | COPY fails on first file | Pre-validate schema; MATCH_BY_COLUMN_NAME for flexibility |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Parquet over CSV | CSV for simplicity | Parquet: columnar, compressed, typed — 3-5x faster COPY INTO |
| COPY INTO over Snowpipe | Snowpipe for continuous | Bulk: COPY INTO with large warehouse is fastest; Snowpipe better for streaming |
| 256MB chunks | Unmodified source files | Optimal parallelism; too small or too large hurts throughput |
| Normalize to Parquet first | Load raw formats directly | Uniform processing; Parquet schema catches errors before Snowflake |
| GCS co-located with Snowflake | Any GCS region | Eliminates egress cost; 5x faster network path |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why normalize to Parquet instead of loading CSV directly?" | "CSV is schemaless — type errors surface at load time, wasting expensive Snowflake compute. Parquet conversion is cheap on GKE pods and catches schema mismatches before we spin up a $2/credit-hour warehouse. It also compresses 3-5x better, reducing both storage cost and network transfer time." |
| "1 TB/hour seems slow for Snowflake" | "That's with an XL warehouse at $32/hour. We can go faster: 4XL (128 nodes) reaches 4 TB/hour at $128/hour. The constraint is cost, not capability. For a one-time migration, the faster speed is justified. For daily incremental, Medium warehouse at 50 GB in 15 minutes is cost-effective." |
| "How do you handle schema changes between loads?" | "MATCH_BY_COLUMN_NAME maps by column name, not position, so column reordering is safe. New columns in the source are caught by the pre-load schema validation step, which either auto-evolves the target table (ALTER TABLE ADD COLUMN) or alerts for human review, depending on the policy." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **three-stage ingestion pipeline**: Normalize → Stage → Load. Raw source data (Oracle DMP, CSV, BCP) is first converted to **Parquet** on GKE pods and split into optimal **256MB chunks** — this gives Snowflake maximum parallelism during COPY INTO. Files are staged to **co-located GCS** with distributed prefixes to avoid hotspots. For bulk loads, **COPY INTO with an XL warehouse** (16 nodes) processes 40,000 files in parallel at 250 MB/sec, completing 10 TB in ~11 hours at $320 in compute. For incremental loads, **Snowpipe** auto-ingests files within 60 seconds via GCS notifications. Post-load validation verifies row counts, checksums, and business invariants. Type mapping from Oracle to Snowflake uses a curated registry with explicit handling for problematic types like CLOB and XMLTYPE."

---

## 11. Key Terms to Drop Naturally

- **COPY INTO**, **Snowpipe**, **Snowpipe Streaming**
- **Parquet**, **columnar format**, **Snappy compression**
- **File sizing** (256MB optimal chunks)
- **GCS prefix distribution** (avoid hotspot)
- **MATCH_BY_COLUMN_NAME**, **ON_ERROR = CONTINUE**
- **Type mapping registry**, **schema evolution**
- **Co-located storage** (same region as compute)
- **Credit consumption**, **warehouse sizing**
