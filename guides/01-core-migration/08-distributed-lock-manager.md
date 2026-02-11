# Design a Distributed Lock Manager

> **Interview Prompt:** "How do you ensure two workers don't convert the same file simultaneously?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How long do locks typically need to be held? (ms vs. minutes vs. hours?) | Determines TTL and heartbeat design |
| 2 | What happens on lock contention? (wait, fail fast, queue?) | Shapes the client-side behavior |
| 3 | Do we need reentrant locks? (same worker re-acquiring) | Complexity of lock implementation |
| 4 | How many concurrent locks? (hundreds vs. millions?) | Storage and performance requirements |
| 5 | What's the cost of a false lock release? (duplicate work vs. data corruption?) | Strictness of safety requirements |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌───────────────────┐     ┌──────────────┐
│  Worker 1 │────▶│                   │     │              │
│  Worker 2 │────▶│   Lock Service    │────▶│  Lock Store  │
│  Worker 3 │────▶│   (Coordinator)   │     │  (Redis/     │
│  Worker N │────▶│                   │     │   ZooKeeper) │
└──────────┘     └───────────────────┘     └──────────────┘
                          │
                          ▼
                  ┌───────────────┐
                  │  Fencing Token │
                  │  Generator     │
                  └───────────────┘
```

---

## 3. Deep-Dive: Lock Implementations

### 3.1 Redis-Based Lock (Simple)

```python
# Acquire lock
ACQUIRED = SET lock:file-123 worker-7-uuid NX EX 30
# NX = only if not exists
# EX 30 = expires in 30 seconds (TTL)

# Release lock (Lua script for atomicity)
EVAL """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
""" 1 "lock:file-123" "worker-7-uuid"
```

**Why Lua for release?**
- Without Lua: Worker A's lock expires → Worker B acquires → Worker A deletes Worker B's lock
- With Lua: Only the lock owner can release it (atomic check-and-delete)

**Pros:** Simple, fast (~1ms), widely understood  
**Cons:** Single Redis node = SPOF; TTL-based expiry can be unsafe

### 3.2 Redlock (Distributed Redis)

```
Lock acquisition across 5 independent Redis nodes:

1. Record start time
2. Try to acquire lock on all 5 nodes with same key + UUID + TTL
3. Lock is acquired if:
   - Majority (3/5) nodes grant the lock
   - Total time < lock TTL
4. Effective TTL = original TTL - elapsed time
5. If lock fails, release on all nodes
```

**Pros:** Tolerates individual node failures  
**Cons:** Controversial (see Martin Kleppmann's critique); clock drift issues

### 3.3 ZooKeeper-Based Lock (Strong Guarantees)

```
Lock acquisition using ephemeral sequential znodes:

1. Create /locks/file-123/lock-  (ephemeral, sequential)
   → ZK returns /locks/file-123/lock-0000000042

2. Get children of /locks/file-123/
   → [lock-0000000040, lock-0000000041, lock-0000000042]

3. Am I the lowest number?
   YES → Lock acquired ✅
   NO  → Watch the node just before mine (lock-0000000041)
          Wait for it to be deleted → retry step 2

4. Release: delete my node (or session expires → ephemeral auto-delete)
```

**Pros:** Linearizable, ephemeral nodes handle worker crashes, no TTL needed  
**Cons:** Slower (~10-50ms), ZooKeeper is complex to operate

### 3.4 etcd-Based Lock (Modern Alternative)

```
1. Create a lease (TTL = 30s) → lease_id
2. PUT /locks/file-123 value=worker-7 lease=lease_id
   (only succeeds if key doesn't exist)
3. Worker sends keep-alive heartbeats to extend lease
4. Release: revoke lease → key auto-deletes
5. Worker crashes: lease expires → lock auto-releases
```

**Pros:** Strong consistency (Raft), simpler than ZooKeeper, modern API  
**Cons:** Requires etcd cluster

### Algorithm Comparison

| Feature | Redis Lock | Redlock | ZooKeeper | etcd |
|---------|-----------|---------|-----------|------|
| Consistency | Weak | Moderate | Strong | Strong |
| Latency | ~1ms | ~5ms | ~10-50ms | ~5-10ms |
| Fault tolerance | None (single node) | Majority quorum | Quorum | Raft quorum |
| Auto-release on crash | TTL-based only | TTL-based | Ephemeral nodes | Lease expiry |
| Complexity | Low | Medium | High | Medium |

---

## 4. Critical Concept: Fencing Tokens

**The Problem:**
```
Time ──────────────────────────────────▶

Worker A: [acquire lock] [GC pause...............] [write result] ← STALE!
Worker B:                 [lock expired] [acquire] [write result] ← CORRECT
```

Worker A's lock expired during a GC pause. It doesn't know it lost the lock and writes stale data.

**The Solution: Fencing Tokens**
```
Lock acquisition returns a monotonically increasing token:
  Worker A acquires lock → fencing_token = 42
  Lock expires, Worker B acquires → fencing_token = 43

Storage system rejects writes with token ≤ last seen token:
  Worker B writes with token 43 → ✅ Accepted
  Worker A writes with token 42 → ❌ Rejected (42 < 43)
```

**This is the most important concept for distributed locks.** Always mention fencing tokens in your interview.

---

## 5. Lock Patterns

### Try-Lock (Non-Blocking)
```python
if lock.try_acquire("file-123"):
    process(file)
    lock.release("file-123")
else:
    skip_or_requeue(file)
```

### Blocking Lock (With Timeout)
```python
if lock.acquire("file-123", timeout=5s):
    process(file)
    lock.release("file-123")
else:
    raise TimeoutError("Could not acquire lock")
```

### Lock with Heartbeat
```python
lock = acquire("file-123", ttl=30s)
heartbeat_thread = start_heartbeat(lock, interval=10s)
try:
    process(file)  # might take minutes
finally:
    stop_heartbeat(heartbeat_thread)
    lock.release()
```

### Read-Write Lock
```python
# Multiple readers OR one exclusive writer
rw_lock = ReadWriteLock("file-123")

# Reader path (concurrent):
rw_lock.read_lock()    # shared lock, many readers allowed
try:
    data = read(file)   
finally:
    rw_lock.read_unlock()

# Writer path (exclusive):
rw_lock.write_lock()   # exclusive lock, blocks all readers + writers
try:
    write(file, new_data)
finally:
    rw_lock.write_unlock()

# Implementation: counter-based
# read_lock: INCR readers_count (if no writer holds lock)
# write_lock: SET writer IF readers_count == 0 AND no writer
```

---

## 6. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Lock contention** (many workers want same lock) | Reduce lock scope (lock per row, not per table) |
| **Dead locks** (Worker A waits for B, B waits for A) | Timeout-based detection; always acquire locks in consistent order |
| **Lock convoy** (queue of waiters behind one lock) | Non-blocking try-lock pattern; backoff with jitter |
| **ZooKeeper/etcd latency** | Cache lock state locally; use Redis for non-critical locks |
| **Monitoring blind spots** | Track lock wait time, hold time, timeout rate; alert on anomalies |

---

## 7. Security Considerations

```
Access Control:
  - Only authenticated workers can acquire locks
  - Per-tenant lock namespaces (tenant-A:file-123 vs tenant-B:file-123)
  - Audit logging: who acquired which lock, when, for how long

Lock Exhaustion Attacks:
  - Rate limit lock acquisition attempts per worker
  - Max locks per worker (prevent one worker from hogging all locks)
  - Auto-release locks held > max TTL (even with heartbeat)

Data Privacy:
  - Lock names may contain sensitive resource IDs
  - Log lock operations without exposing payload
  - Encrypt lock metadata at rest
```

---

## 7. Testing Distributed Locks

```python
# Test: Mutual exclusion
def test_mutual_exclusion():
    lock = DistributedLock("file-123")
    results = []

    def worker(worker_id):
        if lock.acquire(timeout=1):
            results.append(f"{worker_id}-acquired")
            time.sleep(0.5)
            lock.release()
            results.append(f"{worker_id}-released")

    # Run 10 workers concurrently
    threads = [Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify only one worker held lock at a time
    acquired = [r for r in results if "acquired" in r]
    assert len(acquired) > 0
    # Check interleaving pattern: acquired, released, acquired, released...

# Test: Fencing token monotonicity
def test_fencing_tokens():
    tokens = []
    for i in range(5):
        lock = acquire("resource")
        tokens.append(lock.fencing_token)
        lock.release()

    # Verify tokens are monotonically increasing
    assert tokens == sorted(tokens)

# Test: Lock expiration on crash
def test_lock_expiration():
    lock = acquire("file-123", ttl=2)
    assert lock.is_locked()

    # Simulate crash (don't release)
    del lock

    # Wait for TTL expiration
    time.sleep(3)

    # Another worker should now acquire
    lock2 = acquire("file-123", ttl=2)
    assert lock2.is_locked()
```

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if a worker crashes while holding a lock?" | "The lock has a TTL (or uses ZooKeeper ephemeral nodes / etcd leases). If the worker crashes, the session/lease expires and the lock auto-releases. Other workers can then acquire it." |
| "TTL-based locks are unsafe — what if processing takes longer than TTL?" | "Two approaches: (1) Heartbeat-based renewal — the worker extends the TTL periodically while it's still active, (2) Fencing tokens — even if the lock expires and another worker acquires it, the storage system uses fencing tokens to reject writes from the stale lock holder." |
| "Redis isn't consistent enough for locks" | "True — Redis replication is async, so a failover can lose a lock. For critical sections, I'd use etcd or ZooKeeper which provide linearizable consistency. For less critical use cases (like preventing duplicate work), Redis is fine because the worst case is duplicate processing, which is handled by idempotent operations." |
| "What about deadlocks?" | "I prevent deadlocks with two strategies: (1) always acquire locks in a deterministic global order (alphabetical by resource name), and (2) timeout-based deadlock detection — if a lock can't be acquired within 5 seconds, abort and retry with backoff." |
| "How do you choose between Redis and etcd?" | "Redis for performance-critical, best-effort locks (95% of cases). etcd for correctness-critical locks where two workers processing the same resource would cause data corruption. Most systems need both — use the right tool for the risk level." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **distributed lock manager using etcd (or ZooKeeper) for strong consistency cases and Redis for softer locking needs**. Each lock maps to a resource (e.g., a file being converted). Workers acquire locks with a lease/TTL and send heartbeats to extend it during long operations. If a worker crashes, the lease expires and the lock auto-releases. Critically, I'd implement **fencing tokens** — monotonically increasing tokens returned with each lock acquisition. The downstream storage system rejects writes from stale lock holders by comparing fencing tokens. This prevents the classic GC-pause problem where two workers think they hold the same lock. Security includes per-tenant lock namespaces, rate limiting to prevent lock exhaustion attacks, and audit logging of all lock operations."

---

## 10. Key Terms to Drop Naturally

- **Fencing token** (most critical concept)
- **Lease**, **TTL**, **heartbeat**
- **Ephemeral node** (ZooKeeper)
- **Compare-and-Swap (CAS)**
- **Linearizability**, **mutual exclusion**
- **Split-brain**, **clock drift**
- **Read-write lock**, **lock convoy**
- **Deadlock detection**, **lock ordering**
