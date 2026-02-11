# Design an Idempotent API

> **Interview Prompt:** "How do you ensure that retrying a failed API call doesn't cause duplicate side effects?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Which operations need idempotency? (all writes, or specific ones?) | Scope of implementation |
| 2 | What's the retry window? (seconds vs. days?) | Key storage and TTL |
| 3 | Who generates the idempotency key? (client or server?) | API contract design |
| 4 | Are there external side effects? (payments, emails, webhooks?) | Saga/compensation design |
| 5 | What's the concurrency level? (multiple retries in parallel?) | Locking requirements |

---

## 2. High-Level Architecture

```
┌──────────┐                    ┌──────────────────────┐
│  Client   │───── Request ────▶│    API Gateway        │
│           │   Idempotency-Key │                       │
│           │   = "abc-123"     │  1. Check idempotency │
└──────────┘                    │     key in store      │
                                │  2. If seen → return  │
                                │     cached response   │
                                │  3. If new → process  │
                                │     + store response   │
                                └─────────┬─────────────┘
                                          │
                                          ▼
                                ┌─────────────────────┐
                                │  Idempotency Store   │
                                │  (Redis / DB)        │
                                │                       │
                                │  key: "abc-123"       │
                                │  status: COMPLETED    │
                                │  response: {...}      │
                                │  expires: TTL 24h     │
                                └─────────────────────┘
```

---

## 3. Deep-Dive: Core Design

### 3.1 How Idempotency Works

```
First request:
  POST /api/jobs  (Idempotency-Key: xyz-789)
  Body: { "file": "proc.sql", "target": "snowflake" }
  
  → Server checks store: key "xyz-789" not found
  → Process request → Create job → Store response
  → Return: 201 Created { "job_id": "J-42" }
  → Save to store: { key: "xyz-789", status: 201, body: {"job_id": "J-42"} }

Retry (same request):
  POST /api/jobs  (Idempotency-Key: xyz-789)
  Body: { "file": "proc.sql", "target": "snowflake" }
  
  → Server checks store: key "xyz-789" FOUND, status: COMPLETED
  → Return cached: 201 Created { "job_id": "J-42" }
  → No duplicate job created ✅
```

### 3.2 Idempotency Key Design

**Client-generated (recommended):**
```
Idempotency-Key: {UUID v4}
```
- Client generates a unique key per intended action
- Same key = "this is the same request, possibly retried"
- Different key = "this is a new, distinct request"

**Server-generated (alternative):**
```
1. Client: POST /api/jobs/prepare → Server returns token: "tok-123"
2. Client: POST /api/jobs (Token: tok-123) → Execute with idempotency
```

### 3.3 The State Machine

```
Key Received
     │
     ▼
┌─────────┐     ┌──────────┐     ┌──────────┐
│ NOT_FOUND│────▶│ PROCESSING│────▶│ COMPLETED │
│ (new)    │     │ (in-flight)│     │ (done)    │
└─────────┘     └──────────┘     └──────────┘
                     │
                     ▼
               ┌──────────┐
               │  FAILED   │
               │ (errored) │
               └──────────┘
```

**Critical: Handle concurrent retries**
```
Request 1 with key "abc" arrives → Status: PROCESSING
Request 2 with key "abc" arrives (retry) →
  See status = PROCESSING → Wait or return 409 "In Progress"
Request 1 completes → Status: COMPLETED
Request 3 with key "abc" arrives (another retry) →
  See status = COMPLETED → Return cached response
```

### 3.4 Storage Schema

```sql
CREATE TABLE idempotency_keys (
    idempotency_key   VARCHAR(255) PRIMARY KEY,
    user_id           UUID NOT NULL,
    status            ENUM('PROCESSING', 'COMPLETED', 'FAILED'),
    request_path      TEXT,
    request_body_hash VARCHAR(64),   -- SHA-256 of request body
    response_code     INT,
    response_body     JSONB,
    created_at        TIMESTAMP,
    expires_at        TIMESTAMP      -- auto-cleanup after 24-48h
);

-- Or in Redis:
SET idempotency:{key} {json_payload} NX EX 86400
```

### 3.5 Implementation Pattern

```python
def handle_request(request):
    key = request.headers.get('Idempotency-Key')
    if not key:
        return error(400, "Idempotency-Key header required")
    
    # Step 1: Check existing
    existing = idempotency_store.get(key)
    
    if existing:
        if existing.status == 'COMPLETED':
            return existing.response  # Return cached ✅
        if existing.status == 'PROCESSING':
            return error(409, "Request in progress")  # Concurrent retry
        if existing.status == 'FAILED':
            pass  # Allow retry of failed requests
    
    # Step 2: Lock the key (prevent concurrent processing)
    acquired = idempotency_store.set(key, status='PROCESSING', nx=True)
    if not acquired:
        return error(409, "Request in progress")
    
    # Step 3: Process
    try:
        result = process_business_logic(request)
        idempotency_store.update(key, status='COMPLETED', response=result)
        return result
    except Exception as e:
        idempotency_store.update(key, status='FAILED', error=str(e))
        raise
```

---

## 4. Naturally Idempotent Operations

Not everything needs an idempotency key:

| HTTP Method | Naturally Idempotent? | Why |
|-------------|----------------------|-----|
| GET | ✅ Always | Read-only, no side effects |
| PUT | ✅ Usually | "Set X to Y" — same result regardless of repetition |
| DELETE | ✅ Usually | Deleting twice = same end state |
| POST | ❌ Never | Creates new resource each time → needs idempotency key |
| PATCH | ❌ Depends | "Increment by 1" is not idempotent; "set to 5" is |

---

## 5. Handling External Side Effects

The hardest part — what if the operation triggers payments, emails, or webhooks?

```
Request: "Submit migration job" (charges $50)

Step 1: Create job record (DB)           ← idempotent with key
Step 2: Charge payment (Stripe API)      ← external, non-idempotent!
Step 3: Send confirmation email          ← external, non-idempotent!
Step 4: Start worker                     ← idempotent with checks

Solution: Use Stripe's own idempotency key feature
  stripe.charges.create(
      amount=5000,
      metadata={"idempotency_key": "our-key-abc"}
  )
  → Stripe deduplicates the charge on their side
```

For systems without built-in idempotency: use an **outbox pattern** — write intents to a local table, then process them exactly-once with a separate consumer.

### Request Fingerprinting (Alternative to Client Keys)

```
For endpoints where clients can't be trusted to send keys:

Fingerprint = SHA-256(user_id + endpoint + canonical(body))

Example:
  POST /api/jobs
  User: user-123
  Body: { "file": "proc.sql", "target": "snowflake" }

  fingerprint = SHA-256("user-123" + "/api/jobs" + '{"file":"proc.sql","target":"snowflake"}')
            = "a1b2c3d4..."

Pros:
  - No client-side change needed
  - Deterministic (same request = same fingerprint)

Cons:
  - Can't distinguish intentional duplicates (user wants 2 jobs with same input)
  - Body canonicalization is tricky (field order, whitespace)
```

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if the client doesn't send an idempotency key?" | "For critical endpoints (payments, job creation), we require it — return 400 if missing. For non-critical endpoints, we can generate a server-side key from the request hash (path + body + user), though this is less precise." |
| "What about the idempotency store being a bottleneck?" | "We use Redis with TTL-based expiry for fast lookups. Each check is a single O(1) GET. At 10K req/sec, Redis handles this easily. For persistence, we can also write to the database but check Redis first." |
| "How long do you keep idempotency keys?" | "24-48 hours is typical. Long enough to cover network retries and human re-clicks, short enough to not accumulate forever. After expiry, the same key can be reused for a genuinely new request." |
| "What about distributed systems where multiple servers handle the same key?" | "The idempotency store must be centralized (Redis or database), not in-memory on a single server. Using Redis SET NX (set-if-not-exists) gives atomic check-and-lock across all servers. For extra safety, we include a request_body_hash to detect key reuse with different payloads." |
| "What if the server crashes between processing and storing the response?" | "The key is in PROCESSING state. On restart, the background cleaner detects stale PROCESSING keys (older than max processing time) and resets them to allow retry. The business logic must be crash-safe — use DB transactions so partial work is rolled back." |

---

## 7. Summary: Your Interview Narrative

> "I'd implement idempotency using a **client-generated UUID key passed in a request header**. On each request, the server checks an idempotency store (Redis with DB fallback). If the key exists and is COMPLETED, we return the cached response. If PROCESSING, we return 409 to prevent concurrent duplicates. If new, we atomically set status to PROCESSING, execute the business logic, then update to COMPLETED with the response. Keys expire after 24-48 hours. For operations with external side effects (payments, emails), we use the downstream service's own idempotency features or an outbox pattern for exactly-once delivery."

---

## 8. Key Terms to Drop Naturally

- **Idempotency key**, **UUID v4**
- **At-least-once delivery** (why idempotency matters)
- **Compare-And-Swap** (atomic key creation with NX)
- **Outbox pattern** (for external side effects)
- **Exactly-once semantics**
- **409 Conflict** (concurrent retry response)
- **Request fingerprinting**, **body canonicalization**
- **Stale key cleanup**, **background sweeper**
