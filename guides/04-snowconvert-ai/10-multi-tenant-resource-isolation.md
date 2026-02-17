# Multi-Tenant Resource Isolation

> **Interview Prompt:** "SnowConvert serves 50 enterprise clients on a shared GKE cluster. Tenant A submits 200,000 files for conversion; Tenant B needs a single file validated in real time. Design a multi-tenant system that guarantees resource isolation, fair scheduling, and security boundaries."

---

## 1. Requirements

### Functional
- Isolate compute, storage, and network resources between tenants.
- Enforce per-tenant quotas: max concurrent jobs, CPU/memory limits, storage caps.
- Support tiered SLAs: Premium (guaranteed capacity), Standard (best-effort), Free (rate-limited).
- Prevent data leakage: Tenant A cannot access Tenant B's code or conversion results.
- Provide per-tenant billing, usage dashboards, and audit logs.

### Non-Functional
- **Noisy neighbor prevention:** One tenant's batch job cannot degrade other tenants' P99 latency.
- **Scheduling fairness:** Under contention, resources are proportional to SLA tier.
- **Security:** Tenant data encrypted at rest with per-tenant keys (CMEK).
- **Scale:** 50-200 concurrent tenants, 10K total concurrent jobs.

### Capacity Estimation
```
Tenants:           50 enterprise clients
Peak concurrent:   10K jobs across all tenants
Avg per tenant:    200 concurrent jobs (20% active at any time)
GKE cluster:       500 pods, 2000 vCPUs, 8 TB memory
Per-tenant quota:  Premium: 100 pods max, Standard: 20, Free: 5

Storage per tenant: 5-100 GB → Total: 2.5 TB
Network bandwidth:  Premium: 1 Gbps guaranteed, Standard: 100 Mbps
```

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Control Plane (Shared)                        │
│                                                                  │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────────────┐  │
│  │ API Gateway │  │ Tenant Mgr │  │ Resource Quota Manager   │  │
│  │ (Auth +     │  │ (Configs,  │  │ (Enforces limits,        │  │
│  │  Routing)   │  │  SLA tiers)│  │  tracks usage)           │  │
│  └──────┬──────┘  └─────┬──────┘  └────────────┬─────────────┘  │
└─────────│───────────────│──────────────────────│────────────────┘
          │               │                      │
┌─────────▼───────────────▼──────────────────────▼────────────────┐
│                  Compute Layer (GKE)                              │
│                                                                  │
│  Namespace: tenant-42 (Premium)                                  │
│  ┌───────────────────────────────────────┐                      │
│  │ ResourceQuota: 100 pods, 400 CPU, 1.6TB mem                 │
│  │ NetworkPolicy: deny all except own namespace                 │
│  │ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐               │
│  │ │Worker 1│ │Worker 2│ │  ...   │ │Wkr 100│               │
│  │ └────────┘ └────────┘ └────────┘ └────────┘               │
│  └───────────────────────────────────────┘                      │
│                                                                  │
│  Namespace: tenant-43 (Standard)                                 │
│  ┌───────────────────────────────────────┐                      │
│  │ ResourceQuota: 20 pods, 80 CPU, 320GB mem                   │
│  │ NetworkPolicy: deny all except own namespace                 │
│  │ ┌────────┐ ┌────────┐ ┌────────┐                           │
│  │ │Worker 1│ │Worker 2│ │  ...   │                           │
│  │ └────────┘ └────────┘ └────────┘                           │
│  └───────────────────────────────────────┘                      │
│                                                                  │
│  Namespace: tenant-99 (Free)                                     │
│  ┌───────────────────────────────────────┐                      │
│  │ ResourceQuota: 5 pods, 20 CPU, 40GB mem                     │
│  │ LimitRange: max 4 CPU, 8GB RAM per pod                     │
│  │ ┌────────┐ ┌────────┐                                      │
│  │ │Worker 1│ │Worker 2│                                      │
│  │ └────────┘ └────────┘                                      │
│  └───────────────────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────┘
          │               │                      │
┌─────────▼───────────────▼──────────────────────▼────────────────┐
│                  Storage Layer (Per-Tenant Buckets)               │
│                                                                  │
│  gs://snowconvert-tenant-42/  (CMEK: key-tenant-42)            │
│  gs://snowconvert-tenant-43/  (CMEK: key-tenant-43)            │
│  gs://snowconvert-tenant-99/  (CMEK: key-tenant-99)            │
│                                                                  │
│  IAM: Each tenant's pods can ONLY access their own bucket       │
│  via Workload Identity (pod → GCP service account mapping)      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Deep Dive: Isolation Mechanisms

### 3.1 Kubernetes Namespace Isolation

```yaml
# Per-tenant namespace with resource quotas
apiVersion: v1
kind: ResourceQuota
metadata:
  name: tenant-quota
  namespace: tenant-42
spec:
  hard:
    requests.cpu: "400"
    requests.memory: "1600Gi"
    limits.cpu: "400"
    limits.memory: "1600Gi"
    pods: "100"
    services: "10"
    persistentvolumeclaims: "5"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: tenant-limits
  namespace: tenant-42
spec:
  limits:
  - default:
      cpu: "4"
      memory: "8Gi"
    defaultRequest:
      cpu: "1"
      memory: "2Gi"
    max:
      cpu: "16"
      memory: "64Gi"
    type: Container
```

### 3.2 Network Isolation

```yaml
# NetworkPolicy: Deny all ingress/egress except within own namespace
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tenant-isolation
  namespace: tenant-42
spec:
  podSelector: {}    # Apply to all pods in namespace
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: tenant-42
    - namespaceSelector:
        matchLabels:
          name: control-plane    # Allow control plane access
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: tenant-42
  - to:
    - ipBlock:
        cidr: 0.0.0.0/0       # Allow GCS, external APIs
    ports:
    - port: 443
      protocol: TCP
```

### 3.3 Storage Isolation with Workload Identity

```
Per-Tenant Data Access Path:

  Pod (tenant-42 namespace)
    → Kubernetes Service Account: sa-tenant-42
    → Workload Identity binding:
        sa-tenant-42 → GCP SA: tenant-42@project.iam.gserviceaccount.com
    → GCP IAM:
        roles/storage.objectAdmin on gs://snowconvert-tenant-42/
        DENIED on gs://snowconvert-tenant-43/ (no binding)
    
  Result: Pod can ONLY read/write its own tenant's bucket.
  No credentials in pod env vars. No shared service accounts.
```

### 3.4 Encryption: Per-Tenant CMEK

```
Customer-Managed Encryption Keys:
  gs://snowconvert-tenant-42/  → Encrypted with KMS key: projects/p/locations/us/keyRings/kr/cryptoKeys/tenant-42
  gs://snowconvert-tenant-43/  → Encrypted with KMS key: projects/p/locations/us/keyRings/kr/cryptoKeys/tenant-43
  
  Tenant 42's data is encrypted with their own key.
  Revoking the key makes ALL of tenant 42's data unreadable.
  Useful for: data destruction on contract termination.
```

---

## 4. Deep Dive: Fair Scheduling Under Contention

### 4.1 Priority Classes

```yaml
# Premium tenants get higher priority during resource contention
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: premium-tenant
value: 1000
globalDefault: false
description: "Premium SLA tenants - preempt free-tier if needed"
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: standard-tenant
value: 500
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: free-tenant
value: 100
preemptionPolicy: Never   # Free tier pods are never preempted
```

### 4.2 Weighted Fair Queuing

```
Job Queue with tenant fairness:

  Pub/Sub subscriptions per tenant:
    conversion-jobs-tenant-42 (weight: 10, Premium)
    conversion-jobs-tenant-43 (weight: 5,  Standard)
    conversion-jobs-tenant-99 (weight: 1,  Free)

  Scheduler pulls proportionally:
    Of every 16 job pulls:
      10 from tenant-42 (Premium)
      5  from tenant-43 (Standard)
      1  from tenant-99 (Free)
    
  Under low load: All tenants get immediate service.
  Under contention: Resources proportional to tier weight.
```

---

## 5. Deep Dive: Tenant Onboarding & Lifecycle

```
Tenant Onboarding (Automated):
  1. API call: POST /v1/tenants { name: "AcmeCorp", tier: "premium" }
  2. System creates:
     a. Kubernetes namespace: tenant-acme-42
     b. ResourceQuota matching SLA tier
     c. NetworkPolicy for isolation
     d. GCS bucket: gs://snowconvert-tenant-42/
     e. CMEK key in Cloud KMS
     f. Workload Identity binding
     g. Pub/Sub topics/subscriptions
     h. Monitoring dashboard (Grafana)
     i. PostgreSQL schema: tenant_42.* (row-level security)
  3. Total time: ~60 seconds (fully automated via Terraform + K8s operators)

Tenant Offboarding:
  1. Soft delete: Disable API access, suspend all workers
  2. Data retention: Keep data for 90 days (legal compliance)
  3. Hard delete: 
     a. Delete GCS bucket
     b. Revoke CMEK key (renders any cached copies unreadable)
     c. Delete K8s namespace (kills all pods)
     d. Remove Pub/Sub resources
```

---

## 6. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Noisy neighbor (CPU)** | Premium tenant's latency degrades | ResourceQuota + LimitRange enforce hard ceilings per tenant |
| **Tenant exceeds quota** | New pods fail to schedule | Clear 429 error; dashboard shows quota usage; upgrade path |
| **Cross-tenant data leak** | Security violation | WorkloadIdentity + NetworkPolicy + CMEK; audit logging on all data access |
| **KMS key rotation failure** | Can't decrypt tenant data | Automatic key rotation with 90-day schedule; alert on rotation failure |
| **Namespace misconfiguration** | Missing isolation | GitOps (Terraform/Helm): all K8s configs in version control; policy enforcement via OPA/Gatekeeper |

---

## 7. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| K8s namespaces | Separate clusters per tenant | Namespaces: cost-efficient, easier management. Separate clusters: stronger isolation but 10x cost |
| Per-tenant GCS buckets | Shared bucket with prefixes | Separate buckets: IAM at bucket level is simpler; CMEK per bucket |
| Workload Identity | Pod env var credentials | WI: no credential management, auto-rotating tokens |
| Priority Classes | First-come-first-served | Fair scheduling under contention; SLA differentiation |
| NetworkPolicy | VPC per tenant | NetworkPolicy is K8s-native, sufficient for most scenarios |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not separate clusters per tenant?" | "Separate clusters provide stronger isolation but at 10x the cost — each cluster has its own control plane, system pods, and node pools. For 50 tenants, that's 50 control planes at $73/month each = $3,650/month just for overhead. Namespaces with ResourceQuota, NetworkPolicy, and WorkloadIdentity provide sufficient isolation for our threat model." |
| "What if a tenant's pod is compromised?" | "Defense in depth: NetworkPolicy prevents lateral movement to other namespaces. Workload Identity limits GCS access to only the tenant's bucket. CMEK means even if an attacker exfiltrates data from another bucket, they can't decrypt it without the KMS key. GKE Sandbox (gVisor) can add kernel-level isolation for high-security tenants." |
| "How do you handle tenant-specific customizations?" | "Each tenant has a configuration object in the Tenant Manager service — custom retry policies, dialect preferences, webhook URLs, and SLA parameters. Workers read this config at job processing time. The config is cached in Redis with a 5-minute TTL and invalidated on update." |

---

## 9. Summary: Your Interview Narrative

> "I'd design multi-tenancy on GKE using **Kubernetes namespaces** as the primary isolation boundary. Each tenant gets a dedicated namespace with **ResourceQuota** (CPU, memory, pod limits tied to SLA tier), **NetworkPolicy** (deny all except own namespace and control plane), and **Workload Identity** (pod-to-GCS access scoped to tenant's own bucket). Storage uses **per-tenant GCS buckets** encrypted with **CMEK** (Customer-Managed Encryption Keys). Fair scheduling uses **PriorityClasses** — under contention, Premium tenants preempt Free-tier pods. The control plane automates tenant onboarding in ~60 seconds via Terraform: namespace, quotas, bucket, KMS key, Workload Identity, and monitoring dashboard."

---

## 10. Key Terms to Drop Naturally

- **Namespace isolation**, **ResourceQuota**, **LimitRange**
- **NetworkPolicy**, **pod-level network segmentation**
- **Workload Identity** (K8s SA → GCP SA binding)
- **CMEK** (Customer-Managed Encryption Keys)
- **PriorityClass**, **preemption**, **weighted fair queuing**
- **Noisy neighbor**, **blast radius**
- **GKE Sandbox (gVisor)** for kernel isolation
- **Defense in depth**, **least privilege**
