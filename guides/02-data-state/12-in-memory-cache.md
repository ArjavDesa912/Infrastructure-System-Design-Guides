# Design an In-Memory Cache (Redis Architecture)

> **Interview Prompt:** "Design a distributed in-memory cache like Redis."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What data structures? (strings, lists, hashes, sets?) | Scope of the data model |
| 2 | Do we need persistence, or is pure cache OK? | RDB/AOF design decisions |
| 3 | What's the eviction policy? (LRU, LFU, TTL?) | Memory management strategy |
| 4 | Single-node or distributed (cluster mode)? | Sharding and replication |
| 5 | What's the expected hit ratio / cache miss strategy? | Cache-aside vs. read-through |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌──────────────────────────────────────────┐
│  Client   │────▶│              Redis Cluster               │
└──────────┘     │                                          │
                 │  ┌────────┐  ┌────────┐  ┌────────┐     │
                 │  │ Master │  │ Master │  │ Master │     │
                 │  │   A    │  │   B    │  │   C    │     │
                 │  │[0-5460]│  │[5461-  │  │[10923- │     │
                 │  │        │  │ 10922] │  │ 16383] │     │
                 │  └───┬────┘  └───┬────┘  └───┬────┘     │
                 │      │           │           │           │
                 │  ┌───▼────┐  ┌───▼────┐  ┌───▼────┐     │
                 │  │Replica │  │Replica │  │Replica │     │
                 │  │   A'   │  │   B'   │  │   C'   │     │
                 │  └────────┘  └────────┘  └────────┘     │
                 └──────────────────────────────────────────┘
```

### Core Components

1. **Single-threaded event loop** — Processes commands sequentially (no lock contention)
2. **In-memory data structures** — Hash tables, skip lists, zip lists
3. **Hash slot sharding** — 16,384 slots distributed across nodes
4. **Replication** — Async master→replica for read scaling and failover
5. **Persistence** — RDB snapshots + AOF (Append-Only File)

---

## 3. Deep-Dive: Core Design

### 3.1 Why Single-Threaded?

```
Client 1 ──┐
Client 2 ──┤──▶ Event Loop ──▶ [Command Queue] ──▶ Process sequentially
Client 3 ──┤        │
Client N ──┘        ▼
              epoll/kqueue
              (I/O multiplexing)
```

- No locks, no race conditions, no context switching
- Single CPU core can handle **100K+ ops/sec** (bottleneck is memory/network, not CPU)
- I/O multiplexing handles thousands of connections efficiently

### 3.2 Data Structures

| Redis Type | Underlying Structure | When Used |
|-----------|---------------------|-----------|
| String | Simple Dynamic String (SDS) | Small values, counters |
| List | Quick List (linked list of zip lists) | Queues, recent items |
| Hash | Hash Table or ZipList (small hashes) | Object fields |
| Set | Hash Table or IntSet | Unique members, intersections |
| Sorted Set | Skip List + Hash Table | Leaderboards, range queries |

**Skip List** (for Sorted Sets):
```
Level 3: HEAD ─────────────────────────────────── 90
Level 2: HEAD ──────── 30 ─────────────── 70 ─── 90
Level 1: HEAD ── 10 ── 30 ── 50 ── 60 ── 70 ─── 90
Level 0: HEAD ── 10 ── 20 ── 30 ── 40 ── 50 ── 60 ── 70 ── 80 ── 90
```
- O(log N) search, insert, delete
- Simpler than balanced trees, cache-friendly

### 3.3 Eviction Policies

When memory is full, which keys to evict?

| Policy | How It Works | Best For |
|--------|-------------|----------|
| **noeviction** | Reject writes (error) | Critical data |
| **allkeys-lru** | Evict least recently used key | General caching |
| **volatile-lru** | LRU among keys with TTL | Mix of cache + persistent |
| **allkeys-lfu** | Evict least frequently used | Frequency-based access |
| **volatile-ttl** | Evict keys nearest to expiry | TTL-based lifecycle |
| **allkeys-random** | Random eviction | When access is uniform |

**LRU Implementation Detail:**
Redis uses **approximated LRU** — samples 5-10 random keys and evicts the oldest among them. True LRU would require a global linked list (too much memory overhead).

### 3.4 Persistence

**RDB Snapshots:**
```
Every N seconds or M writes → fork() → child writes full dataset to .rdb file
```
- ✅ Compact, fast recovery, good for backups
- ❌ Data loss between snapshots (up to N seconds)

**AOF (Append-Only File):**
```
Every write command → append to AOF file:
  SET user:123 "Alice"
  INCR page_views
  HSET session:abc field1 "val1"
```
- ✅ Minimal data loss (fsync every second or every command)
- ❌ Larger file, slower recovery (replay all commands)

**Recommended:** AOF for durability + periodic RDB for fast recovery backup.

### 3.5 Capacity Estimation

```
Assumptions:
  50 million cached objects
  Average object: 200 bytes key + 1KB value = 1.2KB
  Replication factor: 1 master + 1 replica

Memory:
  50M × 1.2KB = 60GB raw data
  Redis overhead (hash table, pointers): ~50% = 90GB total
  3 master nodes × 30GB each ✅
  3 replica nodes × 30GB each (read scaling + failover)

Throughput:
  Per master: 100K ops/sec
  3 masters = 300K ops/sec total
  p99 latency: < 1ms (in-memory)

Cache hit ratio target: 95%+
  Miss rate: 5% × 300K = 15K DB queries/sec
  Without cache: 300K DB queries/sec (20x reduction)
```

---

## 4. Caching Patterns

### Cache-Aside (Most Common)
```
Read:
  1. Check cache → HIT? Return cached value
  2. MISS? Read from DB → Store in cache → Return

Write:
  1. Write to DB
  2. Invalidate cache (delete key)
```

### Read-Through
```
Read:
  1. Request from cache
  2. Cache auto-fetches from DB on miss
  (Cache manages the DB query)
```

### Write-Through
```
Write:
  1. Write to cache
  2. Cache synchronously writes to DB
  (Strong consistency, higher write latency)
```

### Write-Behind (Write-Back)
```
Write:
  1. Write to cache
  2. Cache asynchronously writes to DB (batched)
  (Lower latency, risk of data loss if cache dies)
```

---

## 5. Cache Invalidation Challenges

| Problem | Description | Solution |
|---------|-------------|----------|
| **Stale data** | Cache has old value | TTL + event-driven invalidation |
| **Cache stampede** | Key expires → 1000 threads hit DB | Lock: first thread fetches, others wait; or probabilistic early expiration |
| **Hot key** | One key gets 100K req/sec | Local in-process cache + replicated hot keys |
| **Cold start** | Empty cache, all requests hit DB | Cache warming on deploy |
| **Inconsistency** | DB updated but cache not invalidated | Change Data Capture (CDC) → invalidate cache |

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "How does single-threaded handle 100K ops/sec?" | "It's single-threaded for command processing, but uses I/O multiplexing (epoll) for network. Operations are in-memory (nanoseconds each), so one CPU core can process 100K+ simple commands/sec. The bottleneck is typically network, not CPU." |
| "What if Redis runs out of memory?" | "We configure maxmemory and an eviction policy (allkeys-lru for caching). When memory limit is hit, Redis evicts old keys to make room. We also monitor memory usage and alert before it's critical." |
| "How do you handle cache consistency with the database?" | "For most use cases, cache-aside with TTLs provides good-enough consistency. For critical data, we use CDC (Change Data Capture) — the database emits change events, and a consumer invalidates the corresponding cache keys in near-real-time." |
| "How do you handle a hot key?" | "Multi-layer defense: (1) local in-process cache (Guava/Caffeine) with short TTL on each application server, (2) replicate the hot key across multiple Redis instances, (3) shard the hot key into sub-keys (e.g., hot_key:1, hot_key:2) and aggregate at read time." |
| "What happens during a Redis node failover?" | "The replica is promoted to master in ~15 seconds. During failover: writes to the failed master's slots are temporarily unavailable, but reads from other slots continue. Application retries with exponential backoff. After promotion, the cluster redirects commands to the new master via MOVED responses." |

---

## 7. Summary: Your Interview Narrative

> "I'd design an **in-memory cache using Redis's proven architecture**: a single-threaded event loop processing commands against in-memory data structures (hash tables, skip lists). The cluster shards data across nodes using 16,384 hash slots with consistent hashing. Each master has replicas for read scaling and failover. Persistence combines AOF (durable, every-second fsync) and RDB snapshots (fast recovery). For eviction, I'd use approximated LRU. The application uses cache-aside pattern — check cache first, fetch from DB on miss. To prevent cache stampede, I'd use distributed locks so only one thread populates a missing key."

---

## 8. Key Terms to Drop Naturally

- **Single-threaded event loop**, **I/O multiplexing** (epoll)
- **Skip list**, **hash table**, **SDS**
- **LRU / LFU eviction**, **approximated LRU**
- **RDB snapshot**, **AOF (Append-Only File)**
- **Cache-aside**, **write-through**, **write-behind**
- **Cache stampede**, **thundering herd**
- **Hash slot**, **consistent hashing**
- **MOVED / ASK** (Redis Cluster redirects)
- **Multi-layer cache** (L1 local + L2 distributed)
