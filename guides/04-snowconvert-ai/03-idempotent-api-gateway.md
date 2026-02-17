# Idempotent API Gateway

> **Interview Prompt:** "A client submits a 500-file batch conversion request. The network drops mid-response. The client retries. How do you ensure you don't process 500 files twice? Design an API gateway that handles retriable vs. non-retriable HTTP codes without duplicating data."

---

## 1. Requirements

### Functional
- Accept an `X-Idempotency-Key` header on all mutating requests (POST, PUT, DELETE).
- Return the cached response for duplicate requests with the same idempotency key.
- Distinguish between retriable (5xx, 429, network errors) and non-retriable (4xx) responses.
- Support key expiration (idempotency keys valid for 24 hours).
- Prevent concurrent duplicate requests (two retries hitting different gateway pods simultaneously).

### Non-Functional
- **Latency overhead:** < 5ms P99 for idempotency check.
- **Throughput:** 5,000 requests/sec across the gateway fleet.
- **Consistency:** Linearizable idempotency guarantees — no duplicate side effects under any race condition.
- **Storage:** Efficient — store only response metadata, not full payloads.

### Capacity Estimation
```
Request rate:          5,000 req/sec
Mutating requests:     ~40% = 2,000 req/sec need idempotency
Idempotency record:    ~500 bytes (key + status + response hash + timestamps)
Records per 24hr TTL:  2,000 × 86,400 = 172.8M records
Storage:               172.8M × 500B = ~86 GB
Redis memory (hot):    Store last 1 hour = 7.2M × 500B = ~3.6 GB
```

---

## 2. API Design

### Request with Idempotency Key
```
POST /v1/conversion/batches
X-Idempotency-Key: batch-7-uuid-abc123
Content-Type: application/json

{
  "tenant_id": "tenant-42",
  "files": [
    {"ref": "gs://uploads/tenant-42/file1.sql", "dialect": "oracle"},
    {"ref": "gs://uploads/tenant-42/file2.sql", "dialect": "oracle"}
  ]
}
```

### First Request → 202 Accepted
```
HTTP/1.1 202 Accepted
X-Idempotency-Applied: false

{
  "batch_id": "batch-99",
  "status": "PROCESSING",
  "job_count": 2
}
```

### Retry (same key) → Cached 202
```
HTTP/1.1 202 Accepted
X-Idempotency-Applied: true

{
  "batch_id": "batch-99",         // Same batch_id — no duplicate
  "status": "PROCESSING",
  "job_count": 2
}
```

### HTTP Status Code Contract

```
Retriable Status Codes (client SHOULD retry):
  429 Too Many Requests    → Rate limited, retry after Retry-After seconds
  500 Internal Server Error → Transient bug
  502 Bad Gateway           → Upstream crash
  503 Service Unavailable   → Overloaded
  504 Gateway Timeout       → Upstream too slow

Non-Retriable Status Codes (client MUST NOT retry):
  400 Bad Request           → Invalid payload (fix the input)
  401 Unauthorized          → Bad credentials
  403 Forbidden             → No permission
  404 Not Found             → Wrong URL
  409 Conflict              → Logical conflict (e.g., already cancelled)
  422 Unprocessable Entity  → Semantic validation failure
```

---

## 3. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Client                                 │
│  (includes idempotency key in X-Idempotency-Key header)      │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────────┐
│                   API Gateway (Envoy + Filter)                │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              Idempotency Middleware                    │    │
│  │                                                       │    │
│  │  1. Extract X-Idempotency-Key from header             │    │
│  │  2. Lookup key in Redis                               │    │
│  │     ├─ KEY NOT FOUND → acquire lock, proceed          │    │
│  │     ├─ KEY FOUND, status=IN_PROGRESS → return 409     │    │
│  │     └─ KEY FOUND, status=COMPLETED → return cached    │    │
│  │  3. After upstream responds:                          │    │
│  │     ├─ 2xx → store response, mark COMPLETED          │    │
│  │     ├─ 4xx → store response, mark COMPLETED (final)  │    │
│  │     └─ 5xx → delete key (allow retry)                 │    │
│  └──────────────────────────────────────────────────────┘    │
│                           │                                   │
│  ┌────────────────────────▼─────────────────────────────┐    │
│  │              Redis Cluster (Idempotency Store)        │    │
│  │                                                       │    │
│  │  Key: "idempotency:{tenant_id}:{idempotency_key}"    │    │
│  │  Value: {                                             │    │
│  │    "status": "COMPLETED",                             │    │
│  │    "response_code": 202,                              │    │
│  │    "response_body_hash": "sha256:abc...",             │    │
│  │    "response_body": "{...}",                          │    │
│  │    "created_at": "2025-01-15T10:00:00Z"              │    │
│  │  }                                                    │    │
│  │  TTL: 24 hours                                        │    │
│  └──────────────────────────────────────────────────────┘    │
│                           │                                   │
└───────────────────────────│───────────────────────────────────┘
                            │
┌───────────────────────────▼───────────────────────────────────┐
│                   Upstream Services                            │
│  (Job Submission, Batch Manager, etc.)                        │
└───────────────────────────────────────────────────────────────┘
```

---

## 4. Deep Dive: Idempotency Protocol

### 4.1 The Race Condition Problem

Without locking, two retries can hit different gateway pods simultaneously:

```
Time    Pod A                           Pod B
────    ─────                           ─────
T1      Lookup key → NOT FOUND          
T2                                      Lookup key → NOT FOUND
T3      Forward to upstream             Forward to upstream
T4      Upstream creates batch-99       Upstream creates batch-100 ← DUPLICATE!
```

### 4.2 Solution: Redis Distributed Lock + Status

```python
def handle_request(key: str, request):
    full_key = f"idempotency:{request.tenant_id}:{key}"
    
    # Step 1: Atomic SET-IF-NOT-EXISTS with lock
    acquired = redis.set(
        full_key,
        json.dumps({"status": "IN_PROGRESS", "locked_by": pod_id, "locked_at": now()}),
        nx=True,      # Only set if not exists
        ex=300         # 5-minute lock timeout (prevents dead locks)
    )
    
    if acquired:
        # Step 2a: We own the lock — forward to upstream
        try:
            response = forward_to_upstream(request)
            
            if response.status_code < 500:
                # 2xx or 4xx → Cache the response (final answer)
                redis.set(full_key, json.dumps({
                    "status": "COMPLETED",
                    "response_code": response.status_code,
                    "response_body": response.body,
                    "completed_at": now()
                }), ex=86400)  # 24-hour TTL
            else:
                # 5xx → Delete the key (allow client to retry)
                redis.delete(full_key)
            
            return response
        except Exception:
            # Upstream unreachable → delete key, allow retry
            redis.delete(full_key)
            raise
    
    else:
        # Step 2b: Key exists — check status
        existing = json.loads(redis.get(full_key))
        
        if existing["status"] == "IN_PROGRESS":
            # Another pod is processing this right now
            return Response(409, "Request is already being processed. Retry later.")
        
        elif existing["status"] == "COMPLETED":
            # Return the cached response
            return Response(
                existing["response_code"],
                existing["response_body"],
                headers={"X-Idempotency-Applied": "true"}
            )
```

### 4.3 Critical: Why 5xx Deletes the Key

| Status Code | Action on Idempotency Key | Rationale |
|-------------|---------------------------|-----------|
| 2xx | Cache response, mark COMPLETED | Success — return same result on retry |
| 400 | Cache response, mark COMPLETED | Client error — retrying with same payload will always fail |
| 401/403 | Cache response, mark COMPLETED | Auth error — same creds will always fail |
| 409 | Cache response, mark COMPLETED | Conflict is deterministic for same input |
| 429 | **DELETE the key** | Transient — should succeed after backoff |
| 500 | **DELETE the key** | Transient — upstream may recover |
| 502/503/504 | **DELETE the key** | Transient — upstream may recover |

**The key insight:** We cache responses for *deterministic* outcomes. 4xx are deterministic (same input → same error). 5xx are *non-deterministic* (same input might succeed next time). Caching a 500 would permanently block the client from retrying successfully.

> *This is a lesson from building Praesidium's compliance validation agents — a validation error (400) is informational and permanent. A timeout hitting the IRS API (504) is transient. Mixing them up means either blocking legitimate retries or caching garbage.*

### 4.4 Handling Missing Idempotency Keys

```
Policy per endpoint:

POST /v1/conversion/batches       → REQUIRED (reject 400 if missing)
POST /v1/jobs                     → REQUIRED
PUT  /v1/jobs/{id}/cancel         → OPTIONAL (operation is naturally idempotent)
GET  /v1/jobs/{id}                → NOT NEEDED (reads are inherently idempotent)
DELETE /v1/dlq/items/{id}         → NOT NEEDED (deleting a deleted item is a no-op)
```

### 4.5 Key Fingerprinting (Preventing Abuse)

An idempotency key should be scoped to a specific request shape. If a client reuses the same key with a different payload, it's a bug:

```python
def validate_key_payload_match(key, request, existing_record):
    """Ensure the same key isn't used for different requests."""
    request_fingerprint = sha256(
        request.method + request.path + request.body
    )
    
    if existing_record.fingerprint != request_fingerprint:
        return Response(422, 
            "Idempotency key reused with different request payload. "
            "Generate a new key for each unique request."
        )
```

---

## 5. Deep Dive: Gateway-Level Concerns

### 5.1 Rate Limiting Integration

```
Rate limiting happens BEFORE idempotency check:

  Request → Rate Limiter → Idempotency Check → Upstream

Why? If we check idempotency first, rate-limited retries would
return cached responses instead of 429, defeating rate limiting.

But for idempotent retries OF a rate-limited response:
  - First call: rate limited → 429, key DELETED (retriable)
  - Client retries after Retry-After delay
  - Second call: rate limiter allows → idempotency key NOT FOUND → proceed
```

### 5.2 Request Deduplication at Scale

```
Redis Cluster Topology for Idempotency:

  6-node Redis Cluster (3 masters, 3 replicas)
  Hash slot distribution: 16,384 slots across 3 masters
  
  Key hashing: HASH("idempotency:{tenant-42}:{uuid-abc}")
    → Slot 8,234 → Master 2
  
  Write: SET with NX → Master 2
  Read:  GET → Replica of Master 2 (for cached responses)
  
  AOF persistence: fsync every second
    → Worst case: 1 second of idempotency keys lost on Redis crash
    → Acceptable: duplicate processing is idempotent at the worker level
```

---

## 6. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Redis down** | Can't check idempotency | Fallback: allow request through (degrade to at-least-once); workers are idempotent anyway |
| **Lock held by dead pod** | Key stuck in IN_PROGRESS | 5-minute lock TTL auto-expires; client can retry after timeout |
| **Client reuses key with different payload** | Could return wrong cached response | Payload fingerprint validation — reject with 422 |
| **Clock skew on TTL** | Keys expire inconsistently | Redis TTL is server-side, not client-side — no clock skew issue |
| **Cache poisoning (cached 500)** | Client permanently blocked | 5xx responses DELETE the key — never cached |
| **Redis memory pressure** | Keys evicted before TTL | Set maxmemory-policy to `volatile-ttl`; only idempotency keys have TTL, so they're evicted first |

---

## 7. Trade-offs & Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Redis for idempotency store | PostgreSQL | 5ms P99 requirement; Redis SET NX is O(1); PG row-level locking adds 10-50ms |
| Delete key on 5xx | Cache 5xx with short TTL | Caching any error response risks permanent failure; clean delete is simpler and safer |
| 409 for concurrent duplicates | Queue the retry internally | 409 is honest — tells client to retry later; internal queuing adds complexity |
| 24-hour key TTL | Infinite retention | 24 hours covers any reasonable retry window; infinite retention wastes memory |
| Payload fingerprinting | Trust client to use keys correctly | Defense in depth — catches bugs in client retry libraries |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just make every endpoint idempotent without a key?" | "Some operations are naturally idempotent (PUT, DELETE), but POST /batches is not — each call creates a new batch. The idempotency key gives the client control: 'This specific call should happen exactly once.' Without it, we'd need server-side deduplication based on payload hashing, which doesn't work for intentional duplicate requests (e.g., two identical batches)." |
| "What if Redis loses the key?" | "Two layers of defense. First, Redis AOF persistence recovers most keys on restart. Second, the downstream workers are idempotent — if a duplicate request slips through, the worker checks the DB for existing results and short-circuits. The gateway idempotency is an optimization to avoid wasting compute, not the sole safety net." |
| "Isn't 409 confusing for the client?" | "It's the correct semantic — 409 Conflict means 'your request conflicts with the current state of the resource,' which in this case is 'another instance of this request is already in-flight.' We include a clear error message and the client's retry library can treat 409 as a short-delay retry. Stripe uses the same pattern." |
| "How do you handle idempotency across API versions?" | "The idempotency key is scoped to the endpoint path and method. A key used on v1/batches and v2/batches are different keys. The fingerprint includes the full URL path, so version migration doesn't cause key collisions." |

---

## 9. Summary: Your Interview Narrative

> "I'd implement idempotency at the **API gateway layer** using Redis as a fast key-value store for idempotency records. Every mutating endpoint requires an `X-Idempotency-Key` header. On first request, we atomically acquire a distributed lock via `SET NX` and forward to the upstream service. On success (2xx) or deterministic failure (4xx), we cache the response with a 24-hour TTL. On transient failure (5xx, 429), we **delete the key** so the client can retry safely. Concurrent duplicates hitting different gateway pods are prevented by the atomic SET NX — the second pod gets a lock failure and returns 409. This gives us **effectively-once semantics** at the API boundary, backed by idempotent workers as a second safety net."

---

## 10. Key Terms to Drop Naturally

- **Idempotency key**, **X-Idempotency-Key header**
- **SET NX** (set-if-not-exists), **distributed lock**
- **Deterministic vs. non-deterministic failure**
- **Retriable (5xx) vs. non-retriable (4xx)**
- **Payload fingerprint**, **cache poisoning**
- **At-most-once, at-least-once, effectively-once**
- **Lock TTL**, **dead lock prevention**
- **409 Conflict**, **Retry-After header**
- **Redis Cluster**, **hash slots**
