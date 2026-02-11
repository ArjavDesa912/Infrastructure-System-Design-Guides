# Design a Code Migration Service

> **Interview Prompt:** "A user uploads a 10GB zip of SQL Server code. Design a system that outputs Snowflake-compatible SQL."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What SQL dialects are we converting from/to? | Scopes parser complexity |
| 2 | What's the expected turnaround time? (minutes vs. hours?) | Determines streaming vs. batch |
| 3 | Do we need semantic validation or just syntax conversion? | Affects whether we need a Snowflake sandbox |
| 4 | Should conversion be all-or-nothing, or can we deliver partial results? | Shapes error handling strategy |
| 5 | How many concurrent migrations? (1 user vs. 1000?) | Drives multi-tenancy and isolation |

---

## 2. High-Level Architecture

```
┌────────────┐    ┌────────────┐    ┌────────────────┐    ┌──────────────┐
│  Upload     │───▶│  Unzip &   │───▶│  Parse &       │───▶│  Convert     │
│  Service    │    │  Inventory │    │  Classify      │    │  (Transpile) │
│  (Resumable)│    │  Worker    │    │  Worker        │    │  Worker      │
└────────────┘    └────────────┘    └────────────────┘    └──────────────┘
       │                │                   │                      │
       ▼                ▼                   ▼                      ▼
┌────────────┐    ┌────────────┐    ┌────────────────┐    ┌──────────────┐
│  Blob Store│    │  File      │    │  Dependency    │    │  Validation  │
│  (S3)      │    │  Registry  │    │  Graph         │    │  Service     │
└────────────┘    └────────────┘    └────────────────┘    └──────────────┘
                                                                  │
                                                                  ▼
                                                          ┌──────────────┐
                                                          │  Package &   │
                                                          │  Deliver     │
                                                          └──────────────┘
```

### Pipeline Stages

1. **Upload Service** — Handles resumable uploads (tus protocol) for large files. Streams directly to S3.
2. **Unzip & Inventory** — Extracts archive, catalogs every `.sql` file with metadata (size, type, path).
3. **Parse & Classify** — Determines object type (table, view, stored procedure, function) per file. Builds dependency graph.
4. **Convert (Transpile)** — Core engine: rewrites SQL syntax from source dialect → target dialect.
5. **Validation** — Runs converted SQL against Snowflake's parser/sandbox for syntax checks.
6. **Package & Deliver** — Bundles output, generates diff report, delivers zip + dashboard.

---

## 3. Deep-Dive: Core Design Decisions

### 3.1 Handling the 10GB Upload

| Challenge | Solution |
|-----------|----------|
| Upload timeout | **Resumable uploads** (tus protocol / multipart S3 upload) — client resumes from last byte |
| Memory pressure | **Stream to S3** directly — never hold full file in memory |
| Network failures | Client-side retry with byte-range tracking |
| Validation | Verify zip integrity with checksum before processing |

### 3.2 Parallelizing the Conversion

A 10GB zip may contain **50,000+ SQL files**. Sequential processing is a non-starter.

```
                    ┌─ Worker 1: tables (batch of 500)
                    ├─ Worker 2: tables (batch of 500)
                    ├─ Worker 3: views  (batch of 200)
Job Orchestrator ───├─ Worker 4: procs  (batch of 100)
                    ├─ Worker 5: procs  (batch of 100)
                    ├─ Worker 6: functions (batch of 300)
                    └─ Worker N: ...
```

**Strategy:**
1. After inventory, group files by **object type** and **dependency level**
2. Convert objects with no dependencies first (tables → views → procs)
3. Within each level, fan out across N workers
4. Use a **job scheduler** (see Guide 01) to orchestrate

### 3.3 The Transpilation Engine

**Option A: AST-Based Transpilation**
```
Source SQL → Parse to AST → Transform AST → Emit Target SQL
```
- ✅ Precise structural transformations
- ❌ Building a full parser for every dialect is expensive

**Option B: LLM-Assisted Conversion**
```
Source SQL → LLM Prompt (with rules + examples) → Target SQL → Validate
```
- ✅ Handles edge cases, natural language in comments
- ❌ Non-deterministic, needs validation loop

**Option C: Hybrid (Recommended)**
```
Source SQL → Rule-based engine (handles 80% of patterns)
          → LLM fallback (handles 20% edge cases)
          → Validation against Snowflake parser
          → Retry loop on failure
```

### 3.4 Dependency Resolution

```
Tables (Level 0) ──┐
                    ├──▶ Views (Level 1) ──┐
                    │                       ├──▶ Procedures (Level 2)
Functions (Level 0)─┘                       │
                                            └──▶ Triggers (Level 2)
```

- Build a **DAG** from `CREATE`, `REFERENCES`, `FROM`, `EXEC` statements
- Topologically sort to determine conversion order
- Circular dependencies → flag for human review

**Dependency Graph Implementation:**
```python
# Pseudo-code for dependency analysis
def build_dependency_graph(sql_files):
    graph = defaultdict(set)  # file -> dependents
    in_degree = defaultdict(int)

    for file in sql_files:
        ast = parse_sql(file.content)
        for ref in extract_references(ast):
            graph[ref].add(file.name)
            in_degree[file.name] += 1

    return graph, in_degree

def topological_sort(graph, in_degree):
    queue = [node for node in graph if in_degree[node] == 0]
    levels = []

    while queue:
        current_level = queue
        queue = []
        for node in current_level:
            for dependent in graph[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        levels.append(current_level)

    return levels  # Each level can be converted in parallel
```

---

## 4. Bottlenecks & Solutions

| Bottleneck | Impact | Solution |
|------------|--------|----------|
| **10GB upload** | Timeout, poor UX | Resumable multipart upload to S3 |
| **50K files to parse** | Hours if serial | Fan-out to 100+ workers, process in parallel |
| **Complex stored procs** | LLM/parser chokes | Retry with more context, fallback to manual queue |
| **Validation against Snowflake** | API rate limits | Batch validation, use local parser first, Snowflake as final check |
| **Output packaging** | Memory if huge | Stream files into output zip on S3 |
| **Dependency cycles** | Can't determine order | Detect cycles in DAG, flag for human resolution |

---

## 5. Data Model

```sql
CREATE TABLE migrations (
    migration_id    UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    source_dialect  VARCHAR(50),
    target_dialect  VARCHAR(50),
    status          ENUM('UPLOADING','EXTRACTING','CONVERTING','VALIDATING','COMPLETE','FAILED'),
    total_files     INT,
    converted_count INT DEFAULT 0,
    failed_count    INT DEFAULT 0,
    input_ref       TEXT,       -- S3 URI
    output_ref      TEXT,       -- S3 URI
    created_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

CREATE TABLE migration_files (
    file_id         UUID PRIMARY KEY,
    migration_id    UUID REFERENCES migrations,
    file_path       TEXT,
    object_type     ENUM('TABLE','VIEW','PROCEDURE','FUNCTION','TRIGGER','OTHER'),
    dependency_level INT,
    status          ENUM('PENDING','CONVERTING','CONVERTED','FAILED'),
    source_ref      TEXT,       -- S3 URI
    output_ref      TEXT,       -- S3 URI
    error_message   TEXT,
    attempts        INT DEFAULT 0
);
```

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "10GB is huge — how do you not run out of memory?" | "We never hold the full file in memory. Uploads stream directly to S3 via multipart. Extraction is streamed. Each worker only loads its assigned files (usually KB-MB each)." |
| "What if the LLM produces incorrect SQL?" | "Every LLM output goes through a validation loop: (1) rule-based syntax check, (2) Snowflake parser check. If validation fails, we retry with the error message as context — up to N times. Persistent failures go to a human review queue." |
| "How do you handle a migration that takes 6 hours?" | "Checkpointing. Every file conversion is independently tracked. If the process crashes at 99%, we resume from the last unconverted file — not from scratch. The user sees real-time progress via WebSocket." |
| "What's the cost of running LLMs at scale?" | "We use a hybrid approach: 80% of files use fast, cheap rule-based conversion. Only the 20% with complex patterns go to LLM. We also cache LLM responses for similar code patterns to avoid redundant calls. Estimated cost: ~$0.50 per GB of SQL code." |
| "How do you ensure semantic equivalence?" | "We don't guarantee semantic equivalence — that's undecidable in general. We provide a diff report showing what changed, and recommend running test suites against converted code. For critical migrations, we offer a side-by-side comparison mode where both source and target code execute on sample data." |
| "What if the zip contains malicious code?" | "Sandbox isolation: each file converts in a container with no network access. We also scan for obvious patterns (DROP TABLE, dangerous system calls) and flag them. The converted code is never executed — only parsed and transformed." |

---

## 7. Cost & Performance Optimization

```
Cost Reduction Strategies:
  1. Spot instances for conversion workers (70% savings)
  2. Batch LLM API calls (process multiple files in one prompt)
  3. Cache conversion results by hash (same SQL = same output)
  4. Auto-suspend idle workers after 5 minutes of inactivity

Performance Targets:
  - Upload: 10GB in < 2 minutes (via multipart)
  - Extraction: 50K files in < 5 minutes
  - Conversion: 500 files/min with 100 workers
  - End-to-end: 1 hour for 10GB zip with 50K files

Optimization Techniques:
  - Compress input ZIP before upload (client-side)
  - Use Snowflake's parser API (not full DB connection)
  - Pre-warm worker pool during upload stage
  - Parallelize validation (10 validators read from queue)
```

---

## 8. Summary: Your Interview Narrative

> "I'd design a **staged pipeline architecture** for code migration. Large files are uploaded via resumable multipart upload to S3. An extraction worker unpacks the archive and catalogs all SQL files into a registry. A dependency analyzer builds a DAG and assigns conversion levels. The transpilation engine — a hybrid of rule-based transforms and LLM fallback — fans out across a worker pool, converting files in dependency order. Every output is validated against Snowflake's parser. Failed conversions retry with error context, and persistent failures go to a DLQ for human review. The whole process is checkpointed per-file, so crashes resume gracefully."

---

## 9. Key Terms to Drop Naturally

- **Resumable upload**, **multipart upload** (tus protocol)
- **DAG** (directed acyclic graph), **topological sort**
- **AST** (abstract syntax tree), **transpilation**
- **Fan-out / fan-in**, **pipeline architecture**
- **Checkpointing**, **idempotent processing**
- **Dead Letter Queue** for unfixable conversions
