# Design a Rate Limiter

> **Interview Prompt:** "Prevent one client from hogging all parser workers in a shared migration platform."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Rate limit by what? (user, API key, IP, tenant?) | Determines the key for tracking |
| 2 | What's the limit? (requests/sec, jobs/hour, concurrent?) | Shapes the algorithm choice |
| 3 | Hard limit (reject) or soft limit (throttle/queue)? | UX and behavior on breach |
| 4 | Distributed or single-node? | Redis vs. in-memory |
| 5 | Do different tiers get different limits? | Need a config/rules engine |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Client   │────▶│  API Gateway      │────▶│  Backend Service  │
│  Request  │     │  (Rate Limiter)   │     │  (Workers)        │
└──────────┘     └──────────────────┘     └──────────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │  Redis       │
                  │  (Counters)  │
                  └──────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │  Rules Store │
                  │  (Limits DB) │
                  └──────────────┘
```

**Where does the rate limiter live?**
- **API Gateway (recommended):** Catches requests before they hit services
- **Service middleware:** Per-service limiting for fine-grained control
- **Client-side:** Cooperative throttling (unreliable, easily bypassed)

Best practice: **both** gateway-level (coarse) + service-level (fine-grained).

---

## 3. Rate Limiting Algorithms

### 3.1 Token Bucket (Most Common)

```
Bucket capacity: 10 tokens
Refill rate: 2 tokens/second

Request arrives:
  if tokens > 0:
      tokens -= 1
      ALLOW
  else:
      REJECT (429 Too Many Requests)

Every 500ms: tokens = min(tokens + 1, capacity)
```

- ✅ **Pros:** Allows bursts (up to bucket capacity), smooth average rate
- ❌ **Cons:** Burst can overwhelm downstream briefly

### 3.2 Sliding Window Log

```
Key: user:123:requests
Value: sorted set of timestamps

Request at time T:
  1. Remove entries older than T - window_size
  2. Count remaining entries
  3. if count < limit: ADD timestamp, ALLOW
     else: REJECT
```

- ✅ **Pros:** Precise, no boundary issues
- ❌ **Cons:** Memory-heavy (stores every timestamp)

### 3.3 Sliding Window Counter (Hybrid — Best Balance)

```
Previous window count: 8  (weight: 0.3 of window remaining)
Current window count:  5

Weighted count = 8 * 0.3 + 5 = 7.4
Limit = 10

7.4 < 10 → ALLOW
```

- ✅ **Pros:** Low memory (two counters), smooth, no boundary spikes
- ❌ **Cons:** Approximate (but good enough for most use cases)

### 3.4 Fixed Window Counter

```
Key: user:123:window:1707500400  (timestamp truncated to minute)
Value: counter

Request arrives:
  INCR counter
  if counter <= limit: ALLOW
  else: REJECT
  EXPIRE key after window_size
```

- ✅ **Pros:** Simplest, lowest memory
- ❌ **Cons:** Boundary problem (2x burst at window edges)

### Algorithm Comparison

| Algorithm | Precision | Memory | Burst Control | Complexity |
|-----------|-----------|--------|---------------|------------|
| Token Bucket | Good | Low | Controlled burst | Medium |
| Sliding Window Log | Exact | High | Perfect | Medium |
| Sliding Window Counter | Approximate | Low | Good | Low |
| Fixed Window | Approximate | Lowest | Poor (boundary) | Lowest |

**Recommended for interviews:** Token Bucket or Sliding Window Counter.

---

## 4. Distributed Rate Limiting with Redis

```python
# Token Bucket in Redis (Lua script for atomicity)
EVAL """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

-- Refill tokens
local elapsed = now - last_refill
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= 1 then
    redis.call('HMSET', key, 'tokens', new_tokens - 1, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1  -- ALLOWED
else
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 0  -- REJECTED
end
""" 1 "ratelimit:user:123" 10 2 1707500400
```

**Why Lua script?**
- Executes atomically on Redis — no race conditions
- Single round trip vs. multiple GET/SET commands

---

## 5. Multi-Tier Rate Limiting

```
Tier 1 (Global):     1000 req/sec across all users
Tier 2 (Per-Tenant):  100 req/sec per tenant
Tier 3 (Per-User):     10 req/sec per user
Tier 4 (Per-Endpoint):  5 req/sec for /convert endpoint per user
```

**Evaluation order:** Check from most specific (Tier 4) to least specific (Tier 1). Reject on first breach.

---

## 6. Response Design

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 2
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1707500460

{
    "error": "rate_limit_exceeded",
    "message": "You have exceeded 100 requests per minute. Please retry after 2 seconds.",
    "retry_after_seconds": 2
}
```

Always return:
- `429` status code
- `Retry-After` header (when the client should retry)
- Rate limit headers so clients can self-throttle

### Graceful Degradation Options

```
Instead of hard rejection, the gateway can:

1. Queue (backpressure):
   Hold request for up to 5s, serve when tokens available
   Good for: batch APIs, non-interactive endpoints
   
2. Degrade quality:
   Serve a cached/stale response instead of fresh computation
   Good for: read-heavy APIs, dashboard data

3. Priority-based shedding:
   Drop lowest-priority requests first (free tier before enterprise)
   Serve 503 with Retry-After to free users

4. Cost-based:
   Each endpoint has a "cost" (GET /user = 1, POST /convert = 10)
   Token bucket deducts by cost, not 1-per-request
   Allows 100 GETs or 10 POSTs per window
```

---

## 7. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Redis single point of failure** | Redis Cluster or Redis Sentinel for HA |
| **Network latency to Redis** | Local in-memory cache with periodic sync (slightly less precise) |
| **Hot key (one user hammering)** | Per-key sharding, or local rate limiting per gateway instance |
| **Clock skew across nodes** | Use Redis server time (not client time) in Lua scripts |
| **Configuration updates** | Rules in a config DB; gateways poll or subscribe to changes |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if Redis goes down?" | "We fail open — allow requests through but log the event. A degraded rate limiter is better than a system-wide outage. We also have local in-memory fallback counters per gateway node." |
| "Isn't checking Redis on every request slow?" | "Redis operations are sub-millisecond. The Lua script runs atomically in one round-trip (~0.5ms). For ultra-low-latency paths, we can use a local token bucket with periodic Redis sync." |
| "How do you handle distributed rate limiting across multiple data centers?" | "Two approaches: (1) Global Redis with cross-DC replication (adds latency), or (2) Per-DC rate limits summing to the global budget (e.g., 3 DCs each get 33% of the quota). Option 2 is simpler and more resilient." |
| "What about variable-cost endpoints?" | "We use weighted token buckets. A GET /user costs 1 token, a POST /convert costs 10 tokens. Same bucket, different consumption rates. This prevents a user from burning their quota with cheap reads when we really need to limit expensive operations." |
| "How do you test rate limiting?" | "Load testing with gradual ramp-up past the limit, verifying 429s appear at the right threshold. Shadow mode: log rate limit decisions without enforcing them, compare against expected behavior. Chaos testing: kill Redis to verify fail-open behavior." |

---

## 9. Summary: Your Interview Narrative

> "I'd implement a **distributed rate limiter using the Token Bucket algorithm backed by Redis**. Each request checks a per-user bucket stored in Redis via an atomic Lua script. The bucket has a capacity (burst limit) and a refill rate (sustained rate). I'd layer this as multi-tier: global → tenant → user → endpoint. The limiter lives at the API Gateway for early rejection, with rejected requests receiving a 429 with Retry-After headers. For high availability, I'd use Redis Cluster and a local in-memory fallback. The rules engine is configurable per tenant tier (free, pro, enterprise)."

---

## 10. Key Terms to Drop Naturally

- **Token Bucket**, **Sliding Window Counter**
- **429 Too Many Requests**, **Retry-After**
- **Lua script** (atomic operations in Redis)
- **Fail open** vs. **fail closed**
- **Backpressure**, **throttling**
- **Fair-share scheduling**
- **Weighted token bucket** (cost-based limits)
- **Shadow mode** (log without enforcing)
