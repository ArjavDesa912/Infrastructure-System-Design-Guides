# Design a Distributed File System

> **Interview Prompt:** "How do you store source code securely and make it accessible across a distributed migration platform?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the typical file size? (KB source files vs. GB datasets?) | Block size and transfer strategy |
| 2 | Read-heavy or write-heavy? | Replication and caching strategy |
| 3 | Do files need to be modified after creation? (append vs. overwrite?) | Immutable vs. mutable design |
| 4 | What's the consistency requirement? | Eventual vs. strong consistency |
| 5 | Multi-tenant with isolation requirements? | Namespace and access control design |

---

## 2. High-Level Architecture (GFS/HDFS-Inspired)

```
┌──────────┐     ┌──────────────────┐
│  Client   │────▶│  Master Node     │
│           │     │  (Name Node)     │
│           │     │  - File metadata  │
│           │     │  - Block mapping  │
│           │     │  - Namespace tree │
└─────┬────┘     └────────┬─────────┘
      │                    │
      │  "Where is file X?"│
      │◀───────────────────┘  "Blocks B1, B2, B3 on Nodes 2, 4, 7"
      │
      │  Direct data transfer (bypasses master)
      │
      ├──────────────────▶ Chunk Server 1 [B1, B5, B9]
      ├──────────────────▶ Chunk Server 2 [B2, B6, B10]
      ├──────────────────▶ Chunk Server 3 [B3, B7, B11]
      └──────────────────▶ Chunk Server N [B4, B8, B12]
```

### Core Components

1. **Master (Name Node)** — Manages metadata: file→block mapping, namespace tree, permissions
2. **Chunk Servers (Data Nodes)** — Store actual file data as fixed-size blocks
3. **Client Library** — Talks to master for metadata, directly to chunk servers for data
4. **Block** — Fixed-size chunk (64MB-256MB), each replicated 3x

---

## 3. Deep-Dive: Core Design

### 3.1 File Storage as Blocks

```
File: "migration_project/src/procedure_A.sql" (200MB)

Split into blocks (64MB each):
  Block B1: bytes 0 - 64MB        → Stored on CS-1, CS-3, CS-5 (3 replicas)
  Block B2: bytes 64MB - 128MB    → Stored on CS-2, CS-4, CS-6
  Block B3: bytes 128MB - 192MB   → Stored on CS-1, CS-4, CS-7
  Block B4: bytes 192MB - 200MB   → Stored on CS-3, CS-5, CS-7

Master metadata:
  /migration_project/src/procedure_A.sql → [B1, B2, B3, B4]
  B1 → {replicas: [CS-1, CS-3, CS-5], size: 67108864, checksum: 0xAF3B...}
```

### 3.2 Read Path

```
1. Client → Master: "I want to read /project/src/file.sql"
2. Master → Client: "File has blocks [B1, B2, B3], here are the chunk server locations"
3. Client → Chunk Server (nearest replica): "Give me block B1"
4. Chunk Server → Client: [data stream]
5. Client verifies checksum
6. Repeat for B2, B3 (can pipeline/parallelize)
```

**Optimization:** Client caches metadata for recently accessed files → skip step 1-2 on subsequent reads.

### 3.3 Write Path

```
1. Client → Master: "I want to write /project/new_file.sql"
2. Master: Allocate block IDs, choose chunk servers for replicas
3. Master → Client: "Write to B-new on CS-2 (primary), CS-5, CS-8"
4. Client → CS-2 (primary): Send data
5. CS-2 → CS-5 → CS-8: Chain replication (pipeline)
6. All three ack → Client confirms write
7. Client → Master: "Write complete"
8. Master: Update metadata (namespace, block map)
```

**Chain replication** minimizes client bandwidth — data flows through the chain once.

### 3.4 Master Availability

The master is a single point of failure. Mitigation:

```
Active Master ◄──── Standby Master
      │                    │
      ▼                    ▼
  Edit Log (WAL)     Replicated WAL
  Checkpoint          Checkpoint

Failover:
  1. Active dies
  2. Standby replays WAL to get latest state
  3. Standby becomes active (~30 seconds)
```

- **Edit log (WAL):** Every metadata change is persisted before ack
- **Checkpoint:** Periodic snapshot of full namespace tree
- **Standby:** Hot standby with replicated WAL for fast failover

---

## 4. Security & Multi-Tenancy

```
Namespace:
  /tenant-A/
      /projects/
          /migration-1/
              /src/
              /converted/
  /tenant-B/
      /projects/
          /migration-2/

Access Control:
  - Tenant-A can ONLY see /tenant-A/**
  - Within tenant: RBAC (admin, developer, viewer)
  - Data encrypted at rest (AES-256 per block)
  - Data encrypted in transit (TLS)
  - Audit log for all access
```

---

## 5. Data Integrity

| Mechanism | How It Works |
|-----------|-------------|
| **Checksums** | Each block has CRC32 checksum, verified on read |
| **Background scrubbing** | Chunk servers periodically re-verify block checksums |
| **Replication** | 3 replicas across different racks/AZs |
| **Under-replication detection** | Master monitors heartbeats; re-replicates if replica count drops |
| **Immutable blocks** | Once written, blocks are never modified (append-only model) |

### Erasure Coding Alternative

```
3x replication: 200% storage overhead (3 copies of every block)

Erasure coding: ~50% overhead (e.g., Reed-Solomon 6+3)
  Split block into 6 data chunks + 3 parity chunks
  Can recover from any 3 chunk failures
  Same durability as 3x replication, but 1.5x storage instead of 3x

Trade-off:
  3x replication: fast reads (just read any copy), simple
  Erasure coding: cheaper storage, but reads need reconstruction

Recommended:
  Hot data (frequently read): 3x replication (fast reads)
  Cold data (archive): Erasure coding (save storage)
```

---

## 6. Capacity Estimation

```
Assumptions:
  100 million files, average 10MB each = 1PB raw data
  Block size: 64MB → 15M blocks
  3x replication: 3PB physical storage
  Chunk servers: 100 nodes × 30TB each = 3PB ✅

Master metadata:
  Each file: ~200 bytes metadata (path, blocks, permissions)
  Each block: ~100 bytes (replicas, checksums)
  100M files × 200B = 20GB
  15M blocks × 100B = 1.5GB  
  Total master memory: ~22GB (fits in RAM) ✅

Throughput:
  Network: 10Gbps per chunk server = 1.25GB/s per node
  100 nodes = 125GB/s aggregate read throughput
  Client read: ~1.25GB/s from single node (saturates network)
```

---

## 7. Garbage Collection & Block Management

```
Garbage collection for deleted files:
  1. Client deletes file → Master marks file as deleted
  2. Master removes file→block mappings
  3. Blocks may still be referenced by snapshots
  4. Background GC scans for unreferenced blocks (refcount = 0)
  5. GC tells chunk servers to delete physical blocks

Block rebalancing:
  Triggered when:
    - Chunk server disk usage > 80%
    - New chunk server added to cluster
    - Chunk server removed from cluster
    
  Process:
    Master identifies blocks to move → least-utilized servers
    Rate-limited to avoid I/O contention (max 10% of disk bandwidth)
```

---

## 8. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Master is SPOF** | Hot standby with WAL replication, automatic failover |
| **Master memory** | All metadata in-memory; may limit total file count. Solution: partition namespace across multiple masters (federated namespace) |
| **Small files** | Many small files waste block space and overwhelm master metadata. Solution: pack small files into archives (HAR files) |
| **Hot data** | Popular files have all reads hitting same replicas. Solution: increase replica count for hot files, client-side caching |
| **Cross-region latency** | Solution: geo-replicated chunk servers, read from nearest replica |
| **Network saturation** | Rate-limit replication and GC operations; use rack-aware placement to minimize cross-rack traffic |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why fixed-size blocks?" | "Fixed blocks simplify replication, load balancing, and space management. The master tracks the same metadata per block regardless of file type. 64MB blocks minimize metadata overhead and are efficient for sequential reads. Small files can be packed." |
| "What if the master fails?" | "The master has a hot standby that continuously replicates the edit log. Failover takes ~30 seconds. During failover, reads from cached metadata still work; only new file creation blocks briefly." |
| "How is this different from S3?" | "Architecturally similar — S3 is essentially a distributed file system with an HTTP API. The key difference is that our system supports a hierarchical namespace, strong consistency on metadata, and can be deployed on-prem. S3 is eventual consistent for some operations and cloud-only." |
| "Why not erasure coding everywhere?" | "Erasure coding saves storage (1.5x vs 3x) but reads need reconstruction from multiple chunks. For hot data, 3x replication gives single-copy reads = lower latency. For cold/archive data, erasure coding is ideal since reads are rare." |
| "How do you handle files smaller than 64MB?" | "Small files still get their own block, but the block only uses actual file size on disk — the 64MB is a maximum, not a fixed allocation. For many small files, we combine them into HAR (Hadoop Archive) files to reduce master metadata." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **GFS-inspired distributed file system** with a master node for metadata and chunk servers for data. Files are split into 64MB blocks, each replicated 3x across different racks. Clients query the master for block locations, then read/write directly to chunk servers — the master never touches data. Writes use chain replication for efficiency. The master is protected by a hot standby with replicated WAL for fast failover. Hot data uses 3x replication for fast reads; cold data uses erasure coding to save storage (1.5x overhead vs. 3x). Multi-tenancy is enforced via isolated namespaces with RBAC. All data is encrypted at rest and in transit, with checksums verified on every read."

---

## 11. Key Terms to Drop Naturally

- **Name Node / Chunk Server** (HDFS terminology)
- **Block**, **replication factor**, **rack awareness**
- **Chain replication**, **write pipeline**
- **WAL (Write-Ahead Log)**, **checkpoint**
- **Checksum**, **data scrubbing**
- **Federated namespace** (for scaling master)
- **Erasure coding** (storage-efficient alternative)
- **HAR files** (small file packing)
