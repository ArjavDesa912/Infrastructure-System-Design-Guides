# Design a Key-Value Store

> **Interview Prompt:** "Design a key-value store with a focus on consistency."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What consistency model? (strong, eventual, causal?) | Core architectural decision |
| 2 | Read-heavy or write-heavy workload? | Determines replication and caching strategy |
| 3 | Expected data size? (fits in memory vs. disk-backed?) | In-memory vs. LSM/B-tree |
| 4 | Do we need transactions? | Single-key atomic vs. multi-key transactions |
| 5 | What's the partition strategy? (range vs. hash?) | Affects range queries and hotspots |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌───────────────────────────────────────┐
│  Client   │────▶│          Coordinator / Router          │
└──────────┘     └───────────────┬───────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │  Node A  │ │  Node B  │ │  Node C  │
              │(Partition│ │(Partition│ │(Partition│
              │  1,2)    │ │  3,4)    │ │  5,6)    │
              └──────────┘ └──────────┘ └──────────┘
                    │            │            │
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Replica  │ │ Replica  │ │ Replica  │
              │  A'      │ │  B'      │ │  C'      │
              └──────────┘ └──────────┘ └──────────┘
```

### Core Components

1. **Coordinator** — Routes requests to correct partition, manages cluster topology
2. **Partition (Shard)** — Subset of the key space, each owned by a node
3. **Replication** — Each partition replicated across N nodes for durability
4. **Storage Engine** — Handles reads/writes on each node (LSM-tree or B-tree)
5. **Consensus** — Ensures consistency across replicas (Raft or Paxos)

---

## 3. Deep-Dive: Core Design Decisions

### 3.1 Partitioning Strategy

**Hash Partitioning:**
```
partition = hash(key) % num_partitions
```
- ✅ Even distribution, no hotspots
- ❌ No range queries, reshuffling on partition count change

**Consistent Hashing (Recommended):**
```
Ring: [0 ─────────────────────────── 2^128]
      │    Node A    │    Node B    │    Node C    │
      
key "user:123" → hash → 0x4F... → lands on Node B

Adding Node D: only keys between C and D move to D
               (minimal data movement)
```
- ✅ Minimal reshuffling when nodes join/leave
- ❌ Can be uneven without virtual nodes

**Virtual Nodes:** Each physical node gets 100-200 positions on the ring → smoother distribution.

### 3.2 Storage Engine: LSM-Tree vs. B-Tree

| Feature | LSM-Tree | B-Tree |
|---------|----------|--------|
| Write speed | ✅ Fast (append-only) | ❌ Slower (in-place update) |
| Read speed | ❌ Slower (check multiple levels) | ✅ Fast (single tree lookup) |
| Space amplification | Higher (duplicates before compaction) | Lower |
| Write amplification | Higher (compaction rewrites) | Lower |
| Best for | Write-heavy (logs, metrics) | Read-heavy (user profiles) |

**LSM-Tree Architecture:**
```
Write Path:
  Key-Value → MemTable (in-memory sorted tree)
                │
           (when full)
                ▼
         Flush to SSTable (sorted, immutable file on disk)
                │
           (background)
                ▼
         Compaction (merge SSTables, remove duplicates)

Read Path:
  Check MemTable → Check L0 SSTables → Check L1 → ... → Check LN
  (Use Bloom filter to skip SSTables that definitely don't have the key)
```

### 3.3 Consistency: Raft Consensus

For strong consistency, use **Raft** for leader-based replication:

```
Write request:
  1. Client → Leader
  2. Leader appends to local log
  3. Leader replicates to followers
  4. Majority (N/2+1) acknowledge → committed
  5. Leader responds to client
  
Read request (linearizable):
  Option A: All reads go through leader (leader checks it's still leader)
  Option B: Read from any node with a "read index" check against leader
```

**Raft guarantees:**
- **Leader election:** If leader fails, followers elect a new leader
- **Log matching:** All committed entries are identical across nodes
- **No split-brain:** Only one leader per term

### 3.4 Read/Write Quorum (Alternative to Raft)

```
N = total replicas = 3
W = write quorum = 2  (must write to 2 nodes to succeed)
R = read quorum = 2   (must read from 2 nodes)

W + R > N → guarantees reading latest write
2 + 2 > 3 ✅ → strong consistency

Trade-offs:
  W=1, R=3: Fast writes, slow reads
  W=3, R=1: Slow writes, fast reads
  W=2, R=2: Balanced
```

---

## 4. API Design

```
PUT /kv/{key}
  Body: { "value": "...", "ttl": 3600 }
  Response: { "version": 42 }

GET /kv/{key}
  Response: { "value": "...", "version": 42, "created_at": "..." }

DELETE /kv/{key}
  Response: { "deleted": true }

CAS (Compare-And-Swap):
PUT /kv/{key}?expected_version=41
  Body: { "value": "new_value" }
  Response: 200 OK (if current version is 41)
            409 Conflict (if version mismatch)
```

---

## 4.1 Capacity Estimation

```
Assumptions:
  1 billion keys
  Average key: 50 bytes, average value: 500 bytes
  Replication factor: 3

Storage:
  1B keys × 550 bytes = 550 GB raw data
  3x replication = 1.65 TB
  10 nodes × 200GB SSD each = 2TB capacity ✅

Memory (LSM MemTable):
  MemTable size: 64MB per partition
  100 partitions per node = 6.4GB RAM for MemTables
  Bloom filters: ~10 bits/key × 100M keys/node = 125MB
  Total RAM per node: ~8GB for data structures

Throughput:
  Write: 100K writes/sec (LSM sequential I/O)
  Read: 500K reads/sec (Bloom filter + SSD)
  Latency: p50 < 1ms, p99 < 10ms
```

### TTL & Expiration

```
How TTL works:
  PUT key=user:123 value="..." ttl=3600
  → Store: {key, value, expires_at: now() + 3600}

Expiration strategies:
  1. Lazy deletion: check expires_at on read, delete if expired
  2. Background sweeper: scan for expired keys every N seconds
  3. Compaction cleanup: during LSM compaction, skip expired entries

Recommended: lazy (immediate correctness) + compaction cleanup (space reclamation)
```

## 5. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Hot key** (one key gets all traffic) | Cache hot keys, replicate hot partitions, application-level sharding |
| **Compaction storms** (LSM) | Rate-limit compaction, use leveled compaction, off-peak scheduling |
| **Leader bottleneck** (Raft) | Partition leadership across nodes, follower reads for stale-OK queries |
| **Rebalancing on node add/remove** | Consistent hashing + virtual nodes → minimal data movement |
| **Large values** | Store value in blob storage, keep reference in KV store |

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use a database?" | "A purpose-built KV store optimized with LSM-trees and Raft offers 10-100x better write throughput than a general-purpose RDBMS. It also allows tunable consistency per read, which an RDBMS doesn't offer." |
| "How do you handle node failures?" | "Data is replicated across 3 nodes. If a node fails, Raft elects a new leader in milliseconds. Reads and writes continue against the remaining quorum. The failed node rejoins and catches up from the leader's log." |
| "What about network partitions?" | "With Raft, the partition containing the majority continues serving requests. The minority side rejects writes (can't form quorum). When the partition heals, the minority catches up. We sacrifice availability in the minority partition to preserve consistency — consistent with the CAP theorem." |
| "How do Bloom filters help?" | "A Bloom filter tells us 'definitely not in this SSTable' with zero false negatives. For a read, instead of scanning all SSTables (O(n) I/O), we check each Bloom filter (in-memory, nanoseconds). Only SSTables that 'might' contain the key get a disk read. This cuts read I/O by 90%+." |
| "How do you handle backup and recovery?" | "Two approaches: (1) Snapshot-based — periodic full copy of LSM files to S3. (2) Log-based — replicate the write-ahead log to a cold standby or S3. For point-in-time recovery, we replay the WAL from a snapshot. Raft log replication already provides durability for operational failures." |

---

## 7. Summary: Your Interview Narrative

> "I'd design a **distributed key-value store using consistent hashing for partitioning and Raft consensus for strong consistency**. Data is partitioned across nodes using consistent hashing with virtual nodes for even distribution. Each partition is replicated to 3 nodes, with Raft ensuring linearizable reads and writes through leader-based replication. The storage engine uses an LSM-tree for high write throughput — writes go to an in-memory MemTable, flush to immutable SSTables, and are compacted in the background. Bloom filters optimize reads by skipping irrelevant SSTables. For reads that can tolerate staleness, we offer follower reads to reduce leader load."

---

## 8. Key Terms to Drop Naturally

- **Consistent hashing**, **virtual nodes**
- **LSM-tree**, **MemTable**, **SSTable**, **Bloom filter**
- **Raft consensus**, **leader election**, **log replication**
- **Quorum** (W+R>N), **linearizability**
- **Compare-And-Swap (CAS)**
- **CAP theorem**, **write amplification**
- **TTL**, **lazy expiration**
- **WAL**, **snapshot**, **point-in-time recovery**
