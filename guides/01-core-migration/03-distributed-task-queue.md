# Design a Distributed Task Queue

> **Interview Prompt:** "Design a system like RabbitMQ or Kafka from scratch."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What ordering guarantees do we need? (FIFO, partial, none?) | Core architectural decision |
| 2 | What's the expected throughput? (msgs/sec) | Determines storage engine choice |
| 3 | Do consumers need to replay messages? | Log-based (Kafka) vs. traditional queue |
| 4 | What delivery semantics? (at-most-once, at-least-once, exactly-once?) | Shapes ack/commit design |
| 5 | Is multi-tenancy required? | Affects isolation and resource management |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌────────────────────────────────────────┐     ┌──────────┐
│ Producers│────▶│           Broker Cluster               │────▶│ Consumers│
│          │     │  ┌──────┐  ┌──────┐  ┌──────┐         │     │          │
│  App A   │     │  │Broker│  │Broker│  │Broker│         │     │  Worker1 │
│  App B   │     │  │  1   │  │  2   │  │  3   │         │     │  Worker2 │
│  App C   │     │  └──────┘  └──────┘  └──────┘         │     │  Worker3 │
└──────────┘     │        Coordination Layer              │     └──────────┘
                 │  ┌──────────────────────────┐          │
                 │  │  ZooKeeper / etcd / Raft  │          │
                 │  └──────────────────────────┘          │
                 └────────────────────────────────────────┘
```

### Two Fundamental Models

| Feature | **Traditional Queue (RabbitMQ-style)** | **Commit Log (Kafka-style)** |
|---------|----------------------------------------|------------------------------|
| Delivery | Message removed after consumption | Message retained, consumers track offset |
| Replay | ❌ Not possible | ✅ Consumers can rewind |
| Ordering | Per-queue FIFO | Per-partition FIFO |
| Scaling | Add more queues | Add more partitions |
| Use case | Task distribution | Event streaming, audit logs |

---

## 3. Deep-Dive: Designing the Commit Log Model

### 3.1 Topic & Partition Design

```
Topic: "conversion-jobs"
├── Partition 0: [msg0, msg1, msg2, msg3, ...]  → Consumer A
├── Partition 1: [msg0, msg1, msg2, msg3, ...]  → Consumer B
├── Partition 2: [msg0, msg1, msg2, msg3, ...]  → Consumer C
└── Partition 3: [msg0, msg1, msg2, msg3, ...]  → Consumer D
```

- **Topic** = logical channel (e.g., "conversion-jobs")
- **Partition** = unit of parallelism and ordering
- Messages within a partition are **strictly ordered**
- Partition assignment: `hash(message_key) % num_partitions`

### 3.2 Storage Engine

```
Partition Directory Structure:
/data/topic-name/partition-0/
    segment-000000000000.log    (immutable, 1GB segments)
    segment-000000000000.index  (offset → file position)
    segment-000001073741824.log
    segment-000001073741824.index
```

**Why append-only logs?**
- Sequential disk writes are **fast** (600MB/s vs 100 IOPS for random)
- Immutable segments enable zero-copy reads via `sendfile()`
- Easy cleanup: delete old segments based on retention policy

**Index structure:**
```
Offset 0    → Position 0
Offset 100  → Position 8192
Offset 200  → Position 16384
...
```
- Sparse index (every Nth offset) → binary search to find message
- Memory-mapped file for speed

### 3.3 Replication

```
Partition 0:
  Leader  (Broker 1)  ◄── Producers write here
  Follower (Broker 2)  ── Replicates from Leader
  Follower (Broker 3)  ── Replicates from Leader
```

- **ISR (In-Sync Replicas):** Followers that are caught up within a threshold
- **acks=all:** Producer waits until all ISR followers confirm
- **Leader election:** If leader dies, promote an ISR follower

### 3.4 Consumer Groups

```
Consumer Group "conversion-workers":
  Consumer A ← Partition 0, 1
  Consumer B ← Partition 2, 3

Consumer Group "audit-logger":
  Consumer X ← Partition 0, 1, 2, 3
```

- Each partition is consumed by **exactly one consumer** in a group
- Different groups independently consume the same topic
- **Rebalancing:** When a consumer joins/leaves, partitions are redistributed

---

## 4. Producer Design

```
Producer.send(topic, key, value):
    1. Serialize key + value
    2. partition = hash(key) % num_partitions  (or round-robin if no key)
    3. Buffer message in batch (accumulator)
    4. When batch is full OR linger.ms expires:
         → Send batch to partition leader
    5. Wait for ack based on configuration:
         acks=0: fire-and-forget
         acks=1: leader confirms write
         acks=all: all ISR replicas confirm
```

---

## 5. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Single partition limits throughput** | Increase partition count; key distribution must be uniform |
| **Consumer slower than producer** | Add consumers (up to partition count); increase batch size |
| **Disk I/O on broker** | Use SSDs, tiered storage (hot→S3), OS page cache |
| **Network between brokers (replication)** | Compress messages (LZ4/Snappy), async replication for non-critical topics |
| **Head-of-line blocking** | Separate topics for different priorities/latency requirements |
| **Consumer rebalance storms** | Sticky partition assignment, cooperative rebalancing |

---

## 6. Delivery Guarantees

| Guarantee | How to Achieve |
|-----------|----------------|
| **At-most-once** | `acks=0`, consumer commits offset before processing |
| **At-least-once** | `acks=all`, consumer commits offset after processing ← **most common** |
| **Exactly-once** | Idempotent producer (sequence numbers) + transactional consumer (read-process-write atomically) |

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use a database as a queue?" | "Databases can work for low-throughput queues, but they suffer from lock contention, polling overhead, and no built-in consumer groups. A dedicated system optimized for sequential I/O, append-only writes, and zero-copy reads will outperform a DB queue by 100x at scale." |
| "How do you handle exactly-once?" | "Idempotent producers with sequence numbers prevent duplicates on the write side. On the consumer side, we use transactional processing — read, process, and commit offset atomically. This is hard to get right, which is why most systems settle for at-least-once with idempotent consumers." |
| "What happens when a broker fails?" | "Partitions are replicated across brokers. If a leader fails, the coordinator promotes an in-sync replica. Producers retry against the new leader. Consumers rejoin the group and get reassigned partitions. Data loss depends on ack settings — with acks=all and min.insync.replicas=2, we lose zero messages." |
| "How do you handle message ordering across partitions?" | "You can't guarantee global ordering across partitions — only per-partition ordering. If you need ordered processing for a specific entity (e.g., all events for user-123), use the entity ID as the partition key. All events for that entity go to the same partition and are processed in order." |
| "What about backpressure?" | "Producer-side: batching with linger.ms and max buffer memory. If the buffer fills, the producer blocks or drops. Consumer-side: if consumers are slow, the lag (offset gap) grows. We monitor consumer lag and auto-scale consumer instances when lag exceeds a threshold." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **commit-log-based distributed task queue** inspired by Kafka. Messages are written to append-only log segments within partitions of a topic. Producers hash a message key to determine the target partition and batch writes for throughput. Each partition is replicated across brokers for durability, with ISR-based leader election. Consumer groups enable parallel processing — each partition is assigned to exactly one consumer in a group, and offsets are tracked per-consumer for replay capability. The storage engine leverages sequential disk I/O and sparse indexes for fast reads. Scaling is achieved by adding partitions and consumers."

---

## 9. Key Terms to Drop Naturally

- **Partition**, **segment**, **offset**
- **ISR (In-Sync Replicas)**, **leader election**
- **Consumer group**, **rebalancing**
- **Zero-copy** (`sendfile`), **sequential I/O**
- **Idempotent producer**, **exactly-once semantics**
- **Backpressure**, **batching**, **linger.ms**
- **Dead letter queue (DLQ)**
- **Tiered storage** (hot/warm/cold)
