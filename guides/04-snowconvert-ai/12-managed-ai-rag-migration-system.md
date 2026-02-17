# Managed AI RAG Migration System

> **Interview Prompt:** "Not every client needs air-gapped infrastructure. Design an API-driven RAG migration system using a managed LLM provider (Anthropic Claude) — focusing on rate limiting, cost control, secure credential management, and context window optimization to minimize API spend."

---

## 1. Requirements

### Functional
- Use Anthropic Claude API for LLM-powered code transpilation.
- Combine Elasticsearch (exact search) and vector DB (semantic search) for retrieval.
- Enforce rate limiting to stay within API rate limits and cost budgets.
- Manage API credentials securely (no plaintext keys, rotation support).
- Optimize context windows to minimize tokens sent (and cost per request).
- Provide per-tenant cost tracking and budget enforcement.

### Non-Functional
- **Cost target:** < $0.05 per file converted (at 500 input + 500 output tokens avg).
- **Throughput:** 100 concurrent conversion requests.
- **Latency:** < 10 seconds P99 per conversion request (including RAG + LLM).
- **Rate limit compliance:** Never exceed Anthropic's 4,000 RPM / 400K TPM limits.
- **Security:** API keys encrypted at rest, accessed via secret manager, rotated quarterly.

### Capacity Estimation
```
Files to convert:      200K files
Avg tokens per file:   500 input + 300 RAG context + 500 output = 1,300 tokens
Total tokens:          200K × 1,300 = 260M tokens
Cost (Claude Sonnet):  Input: $3/MTok, Output: $15/MTok
  Input cost:   200K × 800 tokens × $3/1M = $480
  Output cost:  200K × 500 tokens × $15/1M = $1,500
  Total:        ~$1,980 for full migration
  Per file:     $0.01 avg (well under $0.05 target)

Rate planning:
  RPM limit: 4,000 → max 67 req/sec
  Our target: 30 req/sec (50% headroom for bursts)
  Time for 200K files: 200K / 30 = ~1.8 hours
```

---

## 2. High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    GKE Cluster                                 │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │                RAG Orchestrator                            │ │
│  │         (FastAPI, coordinates full pipeline)              │ │
│  └──────┬──────────┬──────────────┬────────────────────────┘ │
│         │          │              │                           │
│    ┌────▼────┐ ┌───▼──────┐ ┌────▼───────┐                 │
│    │  ES     │ │  Vector  │ │   Prompt   │                  │
│    │ (Exact) │ │  DB      │ │ Optimizer  │                  │
│    │         │ │ (Qdrant) │ │            │                  │
│    └─────────┘ └──────────┘ └────┬───────┘                  │
│                                   │                           │
│    ┌──────────────────────────────▼───────────────────────┐  │
│    │              API Client Layer                         │  │
│    │                                                       │  │
│    │  ┌────────────────┐  ┌────────────────────────────┐  │  │
│    │  │ Rate Limiter   │  │ Credential Manager         │  │  │
│    │  │ (Token Bucket) │  │ (GCP Secret Manager)       │  │  │
│    │  │                │  │                             │  │  │
│    │  │ RPM: 4,000     │  │ API key rotation           │  │  │
│    │  │ TPM: 400,000   │  │ Per-tenant keys            │  │  │
│    │  │ Per-tenant     │  │ Encrypted at rest           │  │  │
│    │  │ quotas         │  │                             │  │  │
│    │  └────────┬───────┘  └──────────────┬─────────────┘  │  │
│    │           │                          │                │  │
│    └───────────│──────────────────────────│────────────────┘  │
│                │                          │                    │
└────────────────│──────────────────────────│────────────────────┘
                 │                          │
                 ▼                          ▼
┌────────────────────────┐  ┌──────────────────────────────────┐
│   Anthropic Claude API  │  │   GCP Secret Manager             │
│   (External)             │  │   (API Keys, Credentials)        │
│                          │  │                                   │
│   POST /v1/messages      │  └──────────────────────────────────┘
│   Rate limits:           │
│   - 4,000 RPM            │
│   - 400,000 TPM          │
└────────────────────────────┘
```

---

## 3. Deep Dive: Rate Limiting (Token Bucket)

### 3.1 Token Bucket Algorithm

```
Token Bucket for API Rate Limiting:

  Bucket capacity:     4,000 tokens (= RPM limit)
  Refill rate:         4,000 tokens per 60 seconds = 66.7 tokens/sec
  Each API call costs: 1 token

  State:
    tokens_available: float = 4000.0
    last_refill_time: timestamp = now()

  On request:
    elapsed = now() - last_refill_time
    tokens_available = min(capacity, tokens_available + elapsed × refill_rate)
    last_refill_time = now()
    
    if tokens_available >= 1.0:
      tokens_available -= 1.0
      → ALLOW request
    else:
      wait_time = (1.0 - tokens_available) / refill_rate
      → REJECT with Retry-After: {wait_time} seconds
```

### 3.2 Dual Token Bucket (RPM + TPM)

```
Anthropic enforces TWO rate limits simultaneously:
  1. Requests per minute (RPM): 4,000
  2. Tokens per minute (TPM): 400,000

We need TWO token buckets:

  RPM Bucket:
    capacity: 4,000
    refill: 66.7/sec
    cost per call: 1

  TPM Bucket:
    capacity: 400,000
    refill: 6,667/sec
    cost per call: estimated_tokens (input + output)

  Request allowed ONLY IF both buckets have capacity.

  Problem: We don't know output tokens until AFTER the call.
  Solution: Estimate output tokens conservatively:
    estimated_output = min(max_tokens, avg_output × 1.5 safety margin)
    Reserve tokens in TPM bucket BEFORE the call
    Reconcile AFTER the call (add back unused tokens)
```

```python
class DualTokenBucket:
    def __init__(self):
        self.rpm_bucket = TokenBucket(capacity=4000, refill_rate=66.7)
        self.tpm_bucket = TokenBucket(capacity=400000, refill_rate=6667)
    
    def acquire(self, estimated_tokens: int) -> tuple[bool, float]:
        """Try to acquire capacity for a request. Returns (allowed, wait_seconds)."""
        rpm_ok, rpm_wait = self.rpm_bucket.try_consume(1)
        tpm_ok, tpm_wait = self.tpm_bucket.try_consume(estimated_tokens)
        
        if rpm_ok and tpm_ok:
            return True, 0
        
        return False, max(rpm_wait, tpm_wait)
    
    def reconcile(self, estimated: int, actual: int):
        """Return unused token capacity after a call completes."""
        if actual < estimated:
            self.tpm_bucket.add(estimated - actual)
```

### 3.3 Handling 429 Too Many Requests

```
Even with our own rate limiter, Anthropic may still return 429:
  - Our rate limiter is approximate (distributed system, not perfectly synced)
  - Anthropic's limits may change dynamically

429 Response Handler:
  HTTP/1.1 429 Too Many Requests
  Retry-After: 30
  x-ratelimit-limit-requests: 4000
  x-ratelimit-remaining-requests: 0
  x-ratelimit-reset-requests: 2025-01-15T10:01:00Z

  Action:
  1. Parse Retry-After header → wait 30 seconds
  2. Temporarily reduce OUR rate limit to 80% of Anthropic's stated limit
  3. Log the 429 with full context
  4. Retry the request after the wait period
  5. If 3 consecutive 429s → reduce to 50% of limit + alert

  Adaptive rate limiting:
    on_429_received():
      self.rpm_bucket.capacity *= 0.8   # Reduce by 20%
      schedule(restore_capacity, delay=300)  # Restore after 5 minutes
```

### 3.4 Per-Tenant Rate Limiting

```
Global budget split across tenants:

  Global: 4,000 RPM
  
  Tenant quotas (by SLA tier):
    Premium:  2,000 RPM (50% of global)
    Standard: 500 RPM per tenant
    Free:     50 RPM per tenant
  
  Total allocated: 2,000 + (5 × 500) + (10 × 50) = 5,000
  Over-committed by 25% → works because not all active simultaneously
  
  If contention → Premium takes priority (weighted fair queuing)
```

---

## 4. Deep Dive: Cost Monitoring & Control

### 4.1 Real-Time Cost Tracking

```
Per-Request Cost Calculation:

  request_cost = (
    input_tokens × input_price_per_token +
    output_tokens × output_price_per_token
  )

  Claude Sonnet pricing (Jan 2025):
    Input:  $3.00 / 1M tokens  = $0.000003 / token
    Output: $15.00 / 1M tokens = $0.000015 / token

  Example: 800 input + 500 output
    Cost = 800 × $0.000003 + 500 × $0.000015 = $0.0099

Aggregated metrics:
  api_cost_dollars_total{tenant="42", model="claude-sonnet"}
  api_tokens_input_total{tenant="42"}
  api_tokens_output_total{tenant="42"}
  api_cost_per_file_dollars{tenant="42"}  (histogram)
```

### 4.2 Budget Enforcement

```
Per-Tenant Budget System:

  Tenant 42 budget: $500/month for API calls

  Budget states:
    0-80%:    Normal operation (green)
    80-95%:   Warning alert to tenant admin (yellow)
    95-100%:  Hard limit — queue requests, alert admin (red)
    >100%:    Blocked — return 402 Payment Required

  Implementation:
    Redis counter: api_spend:{tenant_id}:{month}
    
    Before each API call:
      current_spend = redis.get(f"api_spend:{tenant_id}:2025-01")
      estimated_cost = estimate_cost(input_tokens, max_output_tokens)
      
      if current_spend + estimated_cost > budget:
        return 402, "Monthly API budget exceeded"
      
    After each API call:
      actual_cost = compute_cost(input_tokens, output_tokens)
      redis.incrbyfloat(f"api_spend:{tenant_id}:2025-01", actual_cost)
```

### 4.3 Cost Optimization Dashboard

```
Dashboard Panels:
  1. Total spend (MTD): $1,247.50 / $2,000 budget
  2. Cost per file (P50, P95, P99): $0.008, $0.025, $0.12
  3. Token efficiency: avg input / avg output ratio
  4. Spend by tenant (bar chart)
  5. Spend by dialect (Oracle vs. T-SQL vs. Teradata)
  6. Cost trend (daily spend line chart)
  7. Budget burn rate (projected month-end spend)
```

---

## 5. Deep Dive: Context Window Optimization

### 5.1 The Cost Problem

```
Naive approach:
  Prompt = system_prompt (500 tokens)
        + full_source_file (2000 tokens)
        + 10 RAG examples (10 × 500 = 5000 tokens)  ← EXPENSIVE
        + instructions (200 tokens)
  Total input: 7,700 tokens × $3/MTok = $0.023 per request
  For 200K files: $4,620

Optimized approach:
  Prompt = system_prompt (200 tokens, compressed)
        + source_file (500 tokens, trimmed to relevant section)
        + 3 RAG examples (3 × 200 = 600 tokens, summarized)
        + instructions (100 tokens)
  Total input: 1,400 tokens × $3/MTok = $0.0042 per request
  For 200K files: $840

  Savings: 82% cost reduction from context optimization
```

### 5.2 Optimization Techniques

```
1. Smart Chunking: Only send relevant code sections
   Before: Send entire 2000-line file
   After:  Extract the specific procedure/function (50-200 lines)

2. RAG Result Compression: Summarize retrieved examples
   Before: Full 500-token code examples
   After:  Pattern description + key syntax (100 tokens)
   "Oracle CONNECT BY → Snowflake RECURSIVE CTE. Key mapping:
    CONNECT BY PRIOR parent = child → JOIN in recursive member."

3. System Prompt Caching (Anthropic feature):
   Static system prompt is cached across requests → not re-billed
   Save: 200-500 tokens × $3/MTok × 200K calls = $120-$300

4. Few-Shot Selection by Similarity:
   Don't send 10 generic examples — send 3 most relevant
   Use vector similarity to pick examples that match the input pattern

5. Output Token Bounding:
   Set max_tokens = 2000 (not 4096 default)
   Prevents runaway generation on simple files
   Most conversions complete in < 1000 output tokens
```

### 5.3 Prompt Template

```
System (cached, 200 tokens):
  You are an expert SQL migration assistant specializing in converting
  {source_dialect} to Snowflake SQL. Output ONLY the converted SQL.
  Do not include explanations unless explicitly asked.

User (per-request, variable):
  Convert this {source_dialect} code to Snowflake SQL.
  
  Reference patterns:
  {compressed_rag_examples}   ← 3 examples, ~200 tokens each
  
  Source code:
  ```sql
  {source_code}               ← Trimmed to relevant section
  ```

Max output tokens: 2000
Temperature: 0.1 (deterministic for code)
```

---

## 6. Deep Dive: Secure Credential Management

```
Credential Architecture:

  ┌──────────────────────────┐
  │   GCP Secret Manager     │
  │                           │
  │  Secret: anthropic-api-key│
  │  Version: 3 (current)     │
  │  Version: 2 (previous)    │
  │  Version: 1 (deprecated)  │
  │                           │
  │  Rotation: Every 90 days  │
  │  Auto-rotation via Cloud  │
  │  Function trigger          │
  └─────────┬────────────────┘
            │ IAM: roles/secretmanager.secretAccessor
            │ Only granted to: rag-orchestrator@project.iam.gsa
            ▼
  ┌──────────────────────────┐
  │   RAG Orchestrator Pod    │
  │                           │
  │  Workload Identity →      │
  │  GCP Service Account →    │
  │  Secret Manager access    │
  │                           │
  │  Key loaded at startup    │
  │  Cached in memory (no     │
  │  disk, no env var)         │
  │  Re-fetched on rotation   │
  │  event (Pub/Sub trigger)  │
  └──────────────────────────┘

Security controls:
  ✅ No API keys in source code, env vars, or config files
  ✅ Keys encrypted at rest in Secret Manager (Google-managed key)
  ✅ Access audited via Cloud Audit Logs
  ✅ Automatic rotation every 90 days
  ✅ Per-tenant keys (tenant brings their own Anthropic key)
  ✅ Key usage metrics: track which key is used for which requests
```

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Anthropic API outage** | All LLM calls fail | Circuit breaker; fallback to cached responses for similar queries; queue and retry |
| **429 rate limit exceeded** | Requests throttled | Adaptive rate limiter reduces capacity; Retry-After-aware backoff |
| **Budget exceeded** | Tenant blocked | Early warning at 80%; graceful degradation (queue jobs, slower processing) |
| **API key compromised** | Security breach | Immediate rotation via Secret Manager; audit logs identify scope; revoke old key |
| **Cost spike (prompt injection)** | Unexpectedly large output | max_tokens hard limit; anomaly detection on output length |
| **Model quality regression** | Bad conversions | Canary testing (run 1% through new model version); quality metrics comparison |

---

## 8. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Anthropic Claude over OpenAI | OpenAI GPT-4 | Claude: 200K context window, better code instruction following; competitive pricing |
| Token Bucket over Sliding Window | Fixed window, sliding log | Token bucket is smooth, prevents burst-at-boundary issues |
| GCP Secret Manager over Vault | HashiCorp Vault | GKE-native integration via Workload Identity; zero ops overhead |
| Aggressive context optimization | Send full files | 82% cost reduction at scale; quality impact minimal with smart RAG |
| Per-tenant API keys | Shared org key | Cost attribution; tenant can rotate their own key; compliance |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not self-host and avoid API costs entirely?" | "It depends on the client's constraints. Self-hosting (Guide 11) requires GPU infrastructure ($4K+ for a migration). The managed approach costs ~$2K for 200K files with zero GPU ops. For clients without data sovereignty requirements, the managed approach is cheaper and faster to deploy. We offer both options." |
| "How do you handle API provider switching?" | "The API client layer abstracts the provider behind a common interface. Swapping Anthropic for OpenAI or Google requires changing one config — the prompt template, rate limits, and cost tracking adapt automatically. We've designed for provider portability." |
| "What if Anthropic raises prices?" | "Cost tracking gives us immediate visibility. The per-file cost metric alerts if cost exceeds threshold. The context optimization layer can be tuned — sending fewer/shorter RAG examples reduces cost at a quality trade-off. At extreme price increases, we switch to self-hosted (Guide 11)." |
| "Token bucket rate limiter — what about distributed state?" | "The token bucket state lives in Redis (shared across all orchestrator pods). Redis INCRBYFLOAT is atomic — no race conditions. Redis Cluster provides HA. If Redis is down, we fall back to per-pod in-memory rate limiters at reduced capacity (each pod gets 1/N of the total rate)." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **managed RAG system** with three focus areas: **rate limiting**, **cost control**, and **credential security**. Retrieval uses the same hybrid search (Elasticsearch + Qdrant) as the local system. The API client uses a **dual token bucket** for Anthropic's RPM and TPM limits, with adaptive reduction on 429 responses. **Context window optimization** reduces per-request cost by 82% — smart chunking, RAG compression, few-shot selection by similarity, and Anthropic's prompt caching. Per-tenant **budget enforcement** in Redis blocks requests at 100% spend with early warnings at 80%. API keys are stored in **GCP Secret Manager** with Workload Identity — no keys in code, env vars, or config files, with automatic 90-day rotation. The entire system converts 200K files for ~$2K in API costs with a throughput of 30 req/sec."

---

## 11. Key Terms to Drop Naturally

- **Token Bucket** (dual: RPM + TPM)
- **429 Too Many Requests**, **Retry-After**, **adaptive rate limiting**
- **Context window optimization**, **prompt caching**
- **GCP Secret Manager**, **Workload Identity**
- **CMEK**, **key rotation**
- **Budget enforcement**, **cost per file**
- **Reciprocal Rank Fusion** (hybrid search)
- **max_tokens**, **temperature** (LLM parameters)
- **Provider abstraction** (API portability)
