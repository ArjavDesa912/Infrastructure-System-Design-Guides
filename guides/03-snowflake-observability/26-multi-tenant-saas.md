# Design a Multi-Tenant SaaS Platform

> **Interview Prompt:** "How do you isolate Customer A from Customer B in a shared platform?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How many tenants? (10 enterprise or 100K self-serve?) | Isolation model choice |
| 2 | Compliance requirements? (SOC2, HIPAA, data residency?) | Data storage constraints |
| 3 | Do tenants need different resource limits? | Tier/quota design |
| 4 | Shared infrastructure or dedicated per tenant? | Cost vs. isolation trade-off |
| 5 | Can tenants customize features? | Feature flag complexity |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌──────────────────────────────────────────────┐
│ Tenant A  │────▶│                API Gateway                   │
│ Tenant B  │────▶│  1. Identify tenant (subdomain / API key)    │
│ Tenant C  │────▶│  2. Apply per-tenant rate limits             │
└──────────┘     │  3. Route to appropriate backend              │
                  └──────────────────┬───────────────────────────┘
                                     │
                         ┌───────────┼───────────┐
                         ▼           ▼           ▼
                  ┌──────────┐ ┌──────────┐ ┌──────────┐
                  │App Server│ │App Server│ │App Server│
                  │(Shared)  │ │(Shared)  │ │(Dedicated│
                  │Tenant A,B│ │Tenant B,C│ │Tenant D) │
                  └─────┬────┘ └────┬─────┘ └────┬─────┘
                        │          │              │
                  ┌─────┴──────────┴──────────────┘
                  ▼
         ┌─────────────────────────────────────────┐
         │              Data Layer                  │
         │                                          │
         │  Model 1: Shared DB     (tenant_id col)  │
         │  Model 2: Schema/DB     (per-tenant DB)  │
         │  Model 3: Fully isolated (dedicated infra)│
         └─────────────────────────────────────────┘
```

---

## 3. Isolation Models (Deep Comparison)

### Model 1: Shared Everything (Pooled)
```
┌────────────────────────────────┐
│ Single Database                 │
│                                 │
│ jobs table:                     │
│  id │ tenant_id │ name │ status │
│  1  │ tenant-A  │ ...  │ ...    │
│  2  │ tenant-B  │ ...  │ ...    │
│  3  │ tenant-A  │ ...  │ ...    │
└────────────────────────────────┘

Every query automatically filtered by tenant_id
```

### Model 2: Schema-per-Tenant
```
┌────────────────────────────────────┐
│ Single Database                     │
│  ┌────────────┐ ┌────────────┐     │
│  │ tenant_a.  │ │ tenant_b.  │     │
│  │ jobs       │ │ jobs       │     │
│  │ users      │ │ users      │     │
│  └────────────┘ └────────────┘     │
└────────────────────────────────────┘

Separate schemas, same database instance
```

### Model 3: Database-per-Tenant
```
┌──────────────┐  ┌──────────────┐
│ DB: tenant_a  │  │ DB: tenant_b  │
│  jobs         │  │  jobs         │
│  users        │  │  users        │
└──────────────┘  └──────────────┘

Complete database isolation
```

| Criteria | Shared DB | Schema-per-Tenant | DB-per-Tenant |
|----------|-----------|-------------------|---------------|
| **Cost/tenant** | $ (cheapest) | $$ | $$$ |
| **Data isolation** | Low (app-enforced) | Medium | High |
| **Onboarding speed** | Instant (add row) | Fast (create schema) | Slow (provision DB) |
| **Max tenants** | 100K+ | 1,000s | 100s |
| **Customization** | None | Schema-level | Full |
| **Compliance** | Hard | Medium | Easy |
| **Cross-tenant queries** | Easy (same DB) | Possible | Hard |

---

## 4. Data Isolation: Row-Level Security

```sql
-- PostgreSQL Row-Level Security (RLS)

-- 1. Create policy
CREATE POLICY tenant_isolation ON jobs
    USING (tenant_id = current_setting('app.tenant_id')::UUID);

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

-- 2. Middleware sets tenant context per request
-- (extracted from JWT / API key at gateway)
SET app.tenant_id = 'tenant-123';

-- 3. All queries automatically filtered
SELECT * FROM jobs;
-- Equivalent to: SELECT * FROM jobs WHERE tenant_id = 'tenant-123'

-- Even joins respect RLS:
SELECT j.*, u.name FROM jobs j JOIN users u ON j.user_id = u.id;
-- Both tables filtered by tenant_id automatically

-- Prevents accidental cross-tenant data access even with app bugs ✅
```

---

## 5. Resource Isolation (Noisy Neighbor Prevention)

### Per-Tenant Quotas
```
┌─────────────┬────────┬──────────┬────────────┐
│ Resource     │ Free   │ Pro      │ Enterprise │
├─────────────┼────────┼──────────┼────────────┤
│ API calls    │ 1K/day │ 100K/day │ Unlimited  │
│ Storage      │ 1 GB   │ 100 GB   │ 1 TB       │
│ Concurrency  │ 2 jobs │ 20 jobs  │ 100 jobs   │
│ Workers      │ Shared │ Shared   │ Dedicated  │
│ CPU per query│ 5 sec  │ 30 sec   │ 300 sec    │
│ Support SLA  │ None   │ 24h      │ 1h         │
└─────────────┴────────┴──────────┴────────────┘
```

### Enforcement Layers
```
1. API Gateway: Per-tenant rate limiting (token bucket per tenant)
2. Queue: Per-tenant job concurrency limits
   → Tenant A can only have 20 jobs running simultaneously
3. Compute: Fair-share scheduling
   → If Tenant A submits 1000 jobs, they don't starve Tenant B
4. Storage: Per-tenant quota tracking
   → Reject uploads when quota exceeded
5. Database: Connection pooling per tenant
   → Enterprise tenants get dedicated connection pools
```

### Fair-Share Scheduling
```
Without fair-share:
  Tenant A submits 1000 jobs → occupies all workers → Tenant B starved

With fair-share:
  Worker pool: 100 workers
  Tenant A: 1000 queued jobs → gets 50 workers (fair share)
  Tenant B: 10 queued jobs → gets 10 workers
  Tenant C: 5 queued jobs → gets 5 workers
  Remaining 35 workers → distributed proportionally
```

---

## 6. Tenant Identification & Routing

```
How does the system know which tenant a request belongs to?

Option 1: Subdomain
  tenant-a.app.example.com → tenant_id = "tenant-a"
  
Option 2: API Key / JWT
  Authorization: Bearer <jwt>
  JWT payload: { "tenant_id": "tenant-a", "role": "admin" }

Option 3: Path prefix
  /api/v1/tenants/tenant-a/jobs → tenant_id = "tenant-a"

Recommended: JWT (most flexible, works with both web and API clients)

Gateway middleware:
  1. Extract JWT from Authorization header
  2. Validate JWT signature
  3. Extract tenant_id from claims
  4. Set tenant context for downstream services
  5. Apply tenant-specific rate limits
```

---

## 7. Feature Flags & Customization

```sql
CREATE TABLE tenant_config (
    tenant_id       UUID PRIMARY KEY,
    tier            ENUM('FREE','PRO','ENTERPRISE'),
    features        JSONB,
    custom_domain   TEXT,
    theme           JSONB,
    max_users       INT,
    data_region     VARCHAR(10)
);

-- Example:
INSERT INTO tenant_config VALUES (
    'tenant-123',
    'PRO',
    '{"advanced_analytics": true, "custom_branding": true, "sso": false}',
    'jobs.customer.com',
    '{"primary_color": "#1a73e8"}',
    50,
    'us-east-1'
);
```

---

## 8. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Noisy neighbor** | Per-tenant rate limiting + fair-share scheduling |
| **Data leakage** | RLS at DB level + tenant context middleware + audit logging |
| **Schema migrations** | Shared DB: migrate once. Per-tenant DB: rolling migrations |
| **Onboarding at scale** | Shared DB: instant. Per-tenant: automate provisioning pipeline |
| **Cross-tenant analytics** | Dedicated analytics DB with anonymized/aggregated data |
| **Data residency** | Route tenants to regional databases based on config |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "How do you prevent one tenant from seeing another's data?" | "Three layers: (1) RLS at the database level automatically filters queries by tenant_id, (2) application middleware validates tenant context from JWT before every request, (3) audit logging tracks all data access for compliance review." |
| "What about noisy neighbors?" | "Per-tenant rate limiting at the gateway, per-tenant concurrency limits at the queue, and fair-share scheduling for compute. Enterprise tenants get dedicated worker pools that are isolated from the shared pool." |
| "How do you handle tenant-specific customization?" | "Feature flags stored in a tenant_config table. Each request reads the tenant's config (cached in Redis). Features are toggled per-tenant without code branches — just config-driven behavior." |
| "What if a tenant needs data in a specific region?" | "Each tenant has a data_region in their config. The gateway routes requests to the appropriate regional deployment. For multi-region tenants, we use CRDTs or async replication." |

---

## 10. Summary: Your Interview Narrative

> "I'd use a **shared-compute, tiered-isolation model**. Small tenants share a database with Row-Level Security enforcing data isolation via tenant_id — even buggy application code can't cross tenant boundaries. Enterprise tenants get dedicated databases and optional dedicated compute. A three-layer defense prevents noisy neighbors: rate limiting at the gateway, concurrency limits at the queue, and fair-share scheduling for compute. Tenant context flows from JWT through the entire request lifecycle. Feature flags and per-tenant configuration enable customization without code branches."

---

## 11. Key Terms to Drop Naturally

- **Row-Level Security (RLS)**, **tenant_id**
- **Noisy neighbor problem**, **fair-share scheduling**
- **Feature flags**, **tenant configuration**
- **Resource quotas**, **rate limiting per tenant**
- **Data residency**, **regional routing**
- **Pooled vs. siloed** isolation models
