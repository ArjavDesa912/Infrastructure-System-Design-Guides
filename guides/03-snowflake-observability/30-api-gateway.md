# Design an API Gateway

> **Interview Prompt:** "Design a system that sits between clients and backend services, handling auth, routing, and rate limiting."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How many backend services? | Routing complexity |
| 2 | Expected traffic volume? (req/sec) | Scaling and caching |
| 3 | Authentication model? (API keys, OAuth, JWT?) | Auth pipeline design |
| 4 | Do we need request transformation? | Middleware complexity |
| 5 | Multi-region? | Geo-routing, latency requirements |

---

## 2. High-Level Architecture

```
┌──────────┐
│  Clients  │
│  (Web,    │
│   Mobile, │
│   API)    │
└─────┬────┘
      │ HTTPS
      ▼
┌─────────────────────────────────────────────────────────┐
│                    API Gateway                           │
│                                                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐      │
│  │ TLS     │→│ Rate    │→│  Auth   │→│ Route   │      │
│  │ Termin  │ │ Limit   │ │ (JWT)   │ │ + LB    │      │
│  └─────────┘ └─────────┘ └─────────┘ └────┬────┘      │
│       │           │            │           │            │
│  ┌────┴────┐ ┌────┴─────┐ ┌───┴────┐ ┌────┴──────┐    │
│  │ Logging │ │ Redis    │ │ JWKS   │ │ Service   │    │
│  │ Metrics │ │ (counters│ │ Cache  │ │ Registry  │    │
│  └─────────┘ │ /buckets)│ └────────┘ └───────────┘    │
│              └──────────┘                               │
└──────────────────────────┬──────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │Job       │ │User      │ │File      │
       │Service   │ │Service   │ │Service   │
       │/api/jobs │ │/api/users│ │/api/files│
       └──────────┘ └──────────┘ └──────────┘
```

---

## 3. Deep-Dive: Request Pipeline

### The full processing chain (ordered):

```
Incoming Request
  │
  ├── 1. TLS Termination
  │       Decrypt HTTPS → internal HTTP
  │       Offloads SSL from backend services
  │
  ├── 2. Request Parsing
  │       Parse URL, headers, query params
  │       Extract request ID (or generate one for tracing)
  │
  ├── 3. Rate Limiting
  │       Check per-client/per-tenant limits
  │       → 429 Too Many Requests (if over quota)
  │       → Include Retry-After header
  │
  ├── 4. Authentication
  │       Validate JWT signature (using cached JWKS)
  │       → 401 Unauthorized (if invalid/expired)
  │       Extract user_id, tenant_id, roles from claims
  │
  ├── 5. Authorization
  │       Check if user has permission for this endpoint
  │       → 403 Forbidden (if denied)
  │       RBAC: role → allowed endpoints mapping
  │
  ├── 6. Request Validation
  │       Validate body against OpenAPI schema
  │       → 400 Bad Request (if malformed)
  │
  ├── 7. Request Transformation
  │       Add internal headers (X-Tenant-Id, X-Request-Id)
  │       Rewrite paths (/v2/jobs → /internal/jobs)
  │       Strip sensitive headers before forwarding
  │
  ├── 8. Cache Check
  │       For GET requests: check response cache
  │       → HIT: return cached response (skip backend)
  │
  ├── 9. Service Routing + Load Balancing
  │       Match URL pattern to backend service
  │       Select instance (round-robin / least-connections)
  │       Forward request
  │
  ├── 10. Circuit Breaker
  │        If backend is failing → fail fast with 503
  │        Don't waste time on known-dead services
  │
  ├── 11. Backend Response
  │        Receive response from service
  │        Apply timeout (return 504 if too slow)
  │
  ├── 12. Response Transformation
  │        Add CORS headers
  │        Filter sensitive fields
  │        Compress response (gzip/brotli)
  │
  └── 13. Logging & Metrics
          Log: method, path, status, latency, client_id
          Emit metrics: request_count, latency_histogram
          Emit trace span for distributed tracing
```

---

## 4. Routing Configuration

```yaml
routes:
  - path: /api/v1/jobs/**
    service: job-service
    methods: [GET, POST, PUT, DELETE]
    rate_limit: 100/min
    auth: required
    timeout: 30s
    retry: 2
    circuit_breaker:
      threshold: 5  # failures before open
      cooldown: 60s
    
  - path: /api/v1/users/**
    service: user-service
    methods: [GET, POST]
    rate_limit: 200/min
    auth: required
    timeout: 10s
    
  - path: /api/v1/files/**
    service: file-service
    methods: [GET, POST]
    rate_limit: 50/min
    auth: required
    max_body_size: 100MB  # large file uploads
    timeout: 120s

  - path: /health
    service: gateway-internal
    auth: none
    cache_ttl: 10s
    
  - path: /api/v1/public/**
    service: public-service
    auth: none  # no auth required
    rate_limit: 30/min  # stricter for unauthenticated
    cache_ttl: 60s
```

---

## 5. Circuit Breaker (Critical Pattern)

```
The circuit breaker prevents cascading failures:

States:
  CLOSED (normal) → requests pass through
    │ 5 failures in 30 seconds
    ▼
  OPEN (tripped) → ALL requests fail fast with 503
    │ 60-second cooldown
    ▼
  HALF-OPEN (testing) → allow 1 request through
    │ success?
    ├── YES → back to CLOSED ✅
    └── NO  → back to OPEN ❌

Why it matters:
  Without circuit breaker:
    Backend is down → 1000 requests pile up → each waits 30s timeout
    → Thread pool exhausted → gateway stops handling ALL services
    → One bad service takes down everything

  With circuit breaker:
    Backend is down → circuit opens after 5 failures
    → Next 995 requests fail fast (1ms, not 30s)
    → Gateway stays healthy for other services ✅
```

---

## 6. Service Discovery

```
How does the gateway know WHERE to route?

Option 1: Static config (simple)
  job-service: ["10.0.1.5:8080", "10.0.1.6:8080"]
  
Option 2: DNS-based
  job-service → job-service.internal.example.com
  → DNS returns healthy instances

Option 3: Service registry (Consul / Eureka / K8s)
  Gateway queries registry for "job-service"
  → Returns: [10.0.1.5:8080 (healthy), 10.0.1.6:8080 (healthy)]
  → Load balance across healthy instances
  → Registry updated when instances scale up/down

Recommended: K8s service discovery (if on Kubernetes)
  → Services register automatically
  → Health checks built-in
  → Load balancing via kube-proxy
```

---

## 7. Load Balancing Strategies

| Strategy | How It Works | Best For |
|----------|-------------|----------|
| **Round-robin** | A → B → C → A → B → C | Equal instances |
| **Weighted round-robin** | A(70%) → B(20%) → C(10%) | Canary deployments |
| **Least-connections** | Route to instance with fewest active requests | Variable request duration |
| **IP hash** | Hash client IP → consistent instance | Session affinity |
| **Random** | Pick random instance | Simple, surprisingly good |

---

## 8. Security Considerations

```
Authentication & Authorization:
  - JWT validation at gateway (verify signature, expiration)
  - OAuth2 / OpenID Connect integration
  - Rate limiting per API key + per IP
  - RBAC: check permissions before routing

Input Validation:
  - WAF (Web Application Firewall) rules: SQL injection, XSS detection
  - Request size limits: reject > 10MB payloads
  - Schema validation for POST/PUT (JSON Schema)
  - Path sanitization: prevent directory traversal attacks

DDoS Protection:
  - Per-IP request rate limiting
  - Challenge responses (CAPTCHA) for suspicious traffic
  - Blacklist/whitelist IP ranges
  - Auto-scaling to absorb volumetric attacks
```

---

## 8. Testing API Gateway

```python
# Test: Circuit breaker trips on failures
def test_circuit_breaker():
    # Backend is healthy
    assert circuit_breaker.state == "CLOSED"

    # Simulate 5 failures
    for i in range(5):
        circuit_breaker.record_failure()

    # Should open
    assert circuit_breaker.state == "OPEN"

    # Next request should fail fast
    with pytest.raises(CircuitBreakerOpenError):
        gateway.route("backend-service", "/api/test")

# Test: Rate limiting per tenant
def test_rate_limiting():
    set_rate_limit(tenant_id="tenant-a", limit=10)

    for i in range(10):
        response = gateway.request(tenant_id="tenant-a", path="/api/jobs")
        assert response.status_code == 200

    # 11th request should be rate limited
    response = gateway.request(tenant_id="tenant-a", path="/api/jobs")
    assert response.status_code == 429
    assert "Retry-After" in response.headers

# Test: JWT validation
def test_jwt_validation():
    # Valid token
    valid_jwt = create_jwt(tenant_id="tenant-a")
    response = gateway.request(
        headers={"Authorization": f"Bearer {valid_jwt}"}
    )
    assert response.status_code == 200

    # Expired token
    expired_jwt = create_jwt(tenant_id="tenant-a", exp=-3600)
    response = gateway.request(
        headers={"Authorization": f"Bearer {expired_jwt}"}
    )
    assert response.status_code == 401
```

---

## 9. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Gateway as bottleneck** | Horizontal scaling: multiple gateway instances behind cloud LB |
| **TLS overhead** | Hardware TLS offloading; HTTP/2 multiplexing; TLS session resumption |
| **Auth latency** | Cache JWKS keys locally; JWT validation is local (no auth service call) |
| **Rate limit coordination** | Redis for distributed counters; Lua scripts for atomic operations |
| **Large request bodies** | Stream-through: proxy body without buffering entire payload |
| **Config changes** | Hot-reload routing config without restarting gateway instances |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Isn't the gateway a single point of failure?" | "We run multiple stateless gateway instances behind a cloud load balancer (ALB/NLB). Each instance is identical — rate limit state is in Redis, routing config from a shared store. Any instance handles any request. If one dies, the LB routes to others." |
| "Doesn't it add latency?" | "Typically 1-5ms overhead for the pipeline. The benefits: centralized auth saves each service from implementing it, caching can save 100ms+ on cache hits, and circuit breaking prevents cascading failures that would cause multi-second timeouts." |
| "How do you handle versioning?" | "Path-based (/v1/jobs, /v2/jobs) routing to different backend versions. We can also do header-based routing (Accept-Version: v2). Old versions can run alongside new ones; deprecated versions get 301 redirects or sunset headers." |
| "How do you handle WebSocket connections?" | "The gateway upgrades HTTP to WebSocket and maintains a long-lived connection. It routes based on the initial HTTP upgrade path. WebSocket connections bypass the request pipeline after the initial handshake — they're proxied directly." |

---

## 10. Summary: Your Interview Narrative

> "I'd design the API Gateway as a **stateless reverse proxy** with a 13-step request pipeline: TLS termination → rate limiting (Redis) → authentication (JWT validation, no external call) → authorization (RBAC) → request validation → routing → circuit breaking → response caching. Routes are defined declaratively, mapping URL patterns to backend services with per-route rate limits, timeouts, and retry policies. Circuit breakers prevent cascading failures. Multiple gateway instances run behind a cloud load balancer for high availability — each is stateless and identical."

---

## 11. Key Terms to Drop Naturally

- **Reverse proxy**, **TLS termination**
- **Circuit breaker** (CLOSED → OPEN → HALF-OPEN)
- **Rate limiting** (token bucket / sliding window)
- **Service discovery** (Consul, K8s, DNS)
- **Load balancing** (round-robin, least-connections)
- **JWKS** (JSON Web Key Set for JWT validation)
- **Plugin/middleware pipeline**
- **Request/response transformation**
