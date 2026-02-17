# Local AI RAG Migration System

> **Interview Prompt:** "An enterprise client has strict data sovereignty requirements — their source code cannot leave their network. Design a fully local, open-source RAG system that assists SnowConvert in migrating legacy SQL by providing intelligent code search and AI-powered transpilation suggestions."

---

## 1. Requirements

### Functional
- Run entirely on-premise or within the client's GKE cluster — zero external API calls.
- Provide two search modalities: exact keyword/syntax search (Elasticsearch) and semantic code search (vector DB).
- Host an open-weight LLM locally for AI-assisted transpilation and code explanation.
- Index an entire legacy codebase (50K–500K files) for retrieval-augmented generation.
- Support iterative migration: user queries the system, reviews suggestions, and refines.

### Non-Functional
- **Data privacy:** No data leaves the cluster. All processing is local.
- **Query latency:** < 5 seconds for search + LLM generation.
- **Indexing throughput:** Index 100K files in < 2 hours.
- **Model quality:** Comparable to GPT-4 for SQL transpilation tasks.
- **Uptime:** 99.9% within the client's infrastructure.

### Capacity Estimation
```
Codebase:          200K SQL files, avg 500 lines, avg 25KB
Total source:      200K × 25KB = 5 GB text
Elasticsearch:     5 GB raw → ~8 GB indexed (with analyzers)
Vector DB:         200K files × avg 10 chunks × 768-dim float32
                   = 2M vectors × 768 × 4 bytes = ~6 GB
LLM serving:       DeepSeek-R1 67B (Q4 quantized) → ~35 GB VRAM
                   or GLM-4 9B → ~6 GB VRAM (lighter option)
GPU requirement:   2× NVIDIA A100 80GB (for 67B model)
                   or 1× NVIDIA L4 24GB (for 9B model)
```

---

## 2. High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│              Client's GKE Cluster (Air-Gapped)                 │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │                    RAG Orchestrator                        │ │
│  │          (FastAPI service, coordinates pipeline)          │ │
│  └─────┬──────────┬──────────────┬──────────────┬───────────┘ │
│        │          │              │              │             │
│   ┌────▼────┐ ┌───▼──────┐ ┌────▼───────┐ ┌───▼───────────┐│
│   │Ingestion│ │   Search  │ │   Search   │ │  LLM Serving  ││
│   │Pipeline │ │  (Exact)  │ │ (Semantic) │ │  (vLLM)       ││
│   │         │ │           │ │            │ │               ││
│   │ Reads   │ │Elastic-   │ │ Qdrant /   │ │ DeepSeek-R1   ││
│   │ source  │ │search     │ │ Milvus     │ │ or GLM-4      ││
│   │ files → │ │           │ │            │ │               ││
│   │ chunks  │ │ Keyword   │ │ Embedding  │ │ GPU Node Pool ││
│   │ → embed │ │ + syntax  │ │ similarity │ │ (A100 / L4)   ││
│   │ → index │ │ matching  │ │ search     │ │               ││
│   └─────────┘ └───────────┘ └────────────┘ └───────────────┘│
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │                    Storage Layer                           │ │
│  │  - PVC (Persistent Volume): source files, model weights   │ │
│  │  - Elasticsearch data volume: 50 GB                       │ │
│  │  - Qdrant/Milvus data volume: 20 GB                       │ │
│  └──────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

---

## 3. Deep Dive: Dual Search Architecture

### 3.1 Elasticsearch (Exact Keyword / Syntax Matching)

```
Use case: Find all files that use Oracle's CONNECT BY clause.

Query: "CONNECT BY PRIOR" 
  → Elasticsearch full-text search with custom SQL analyzer
  → Returns: 47 files containing this exact syntax

Index configuration:
  {
    "mappings": {
      "properties": {
        "file_path":    { "type": "keyword" },
        "content":      { "type": "text", "analyzer": "sql_analyzer" },
        "dialect":      { "type": "keyword" },
        "object_type":  { "type": "keyword" },  // procedure, function, view
        "line_count":   { "type": "integer" },
        "dependencies": { "type": "keyword" }    // referenced tables/procs
      }
    },
    "settings": {
      "analysis": {
        "analyzer": {
          "sql_analyzer": {
            "type": "custom",
            "tokenizer": "sql_tokenizer",
            "filter": ["lowercase", "sql_keywords"]
          }
        },
        "tokenizer": {
          "sql_tokenizer": {
            "type": "pattern",
            "pattern": "[\\s,();]+"   // Split on SQL delimiters
          }
        }
      }
    }
  }
```

**When to use Elasticsearch:** Exact syntax patterns, keyword searches, regex matching, finding specific function calls, filtering by metadata (dialect, file type).

### 3.2 Vector Database (Semantic Code Search)

```
Use case: "Find all stored procedures that calculate order totals 
          with discounts and tax" — even if the code uses different 
          variable names, comments, or structure.

Flow:
  1. Embed query with CodeBERT / StarCoder embedding model
     → query_vector = embed("procedures that calculate order totals...")
     → [0.12, -0.34, 0.56, ..., 0.78]  (768 dimensions)
  
  2. Search Qdrant for nearest neighbors
     → Top 10 most semantically similar code chunks
     → Includes code with names like "calc_invoice_total", 
       "compute_final_amount", "get_order_summary"
  
  3. Return ranked results with similarity scores

Chunking strategy for code:
  - Split by function/procedure boundaries (AST-aware chunking)
  - Each chunk: one function/procedure + its docstring/comments
  - Overlap: include function signature in adjacent chunks
  - Max chunk size: 512 tokens (embedding model limit)
  
  Example chunk:
    -- Calculates the final order amount with discounts and tax
    CREATE PROCEDURE calc_order_total(p_order_id IN NUMBER) AS
      v_subtotal  NUMBER;
      v_discount  NUMBER;
      v_tax       NUMBER;
    BEGIN
      SELECT SUM(quantity * unit_price) INTO v_subtotal FROM order_items WHERE order_id = p_order_id;
      v_discount := get_discount(p_order_id);
      v_tax := v_subtotal * 0.08;
      UPDATE orders SET total = v_subtotal - v_discount + v_tax WHERE order_id = p_order_id;
    END;
```

**When to use Vector DB:** Semantic similarity ("find code that does X"), finding analogous patterns across dialects, discovering similar business logic.

### 3.3 Hybrid Search (Reciprocal Rank Fusion)

```
Combine Elasticsearch and Vector DB results:

  ES results:     [doc_A (rank 1), doc_B (rank 2), doc_C (rank 3)]
  Qdrant results: [doc_B (rank 1), doc_D (rank 2), doc_A (rank 3)]

  RRF score: score(doc) = Σ 1/(k + rank(doc, result_set))
             where k = 60 (constant)

  doc_A: 1/(60+1) + 1/(60+3) = 0.0164 + 0.0159 = 0.0323
  doc_B: 1/(60+2) + 1/(60+1) = 0.0161 + 0.0164 = 0.0325  ← WINNER
  doc_C: 1/(60+3) + 0        = 0.0159
  doc_D: 0        + 1/(60+2) = 0.0161

  Final ranked results: [doc_B, doc_A, doc_D, doc_C]
```

---

## 4. Deep Dive: LLM Infrastructure on GKE

### 4.1 GPU Node Pool Configuration

```yaml
# GKE GPU node pool for LLM serving
apiVersion: container.googleapis.com/v1
kind: NodePool
metadata:
  name: gpu-llm-pool
spec:
  config:
    machineType: a2-highgpu-2g  # 2× A100 80GB
    accelerators:
    - acceleratorCount: 2
      acceleratorType: nvidia-tesla-a100
    diskSizeGb: 500              # Model weights + KV cache
    diskType: pd-ssd
  initialNodeCount: 1
  autoscaling:
    enabled: true
    minNodeCount: 0               # Scale to zero when no queries
    maxNodeCount: 3               # Scale up for batch workloads
```

### 4.2 vLLM Serving Configuration

```yaml
# vLLM deployment for DeepSeek-R1 67B
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llm-server
  namespace: rag-system
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: vllm
        image: vllm/vllm-openai:latest
        args:
        - --model=/models/deepseek-r1-67b-q4
        - --tensor-parallel-size=2      # Split across 2 GPUs
        - --max-model-len=32768         # 32K context window
        - --gpu-memory-utilization=0.90
        - --enforce-eager                # Disable CUDA graphs (saves VRAM)
        - --quantization=awq            # 4-bit quantization
        resources:
          limits:
            nvidia.com/gpu: 2
            memory: "160Gi"
          requests:
            nvidia.com/gpu: 2
            memory: "80Gi"
        volumeMounts:
        - name: model-storage
          mountPath: /models
      nodeSelector:
        cloud.google.com/gke-accelerator: nvidia-tesla-a100
      tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
```

### 4.3 Handling OOM Errors

```
OOM Prevention Strategy:

  1. Request-level memory budgeting:
     - Track KV cache memory per request
     - Reject requests that would exceed VRAM budget
     - Return 503 with Retry-After header
  
  2. Dynamic batching with max_num_seqs:
     - vLLM batches concurrent requests
     - Limit: max 8 concurrent sequences
     - If batch full → queue with 2s timeout
  
  3. Context window management:
     - Limit input context to 16K tokens (even though model supports 32K)
     - Reserve remaining 16K for generation
     - If code chunk exceeds 16K → summarize before sending to LLM
  
  4. Fallback chain:
     Primary:   DeepSeek-R1 67B (best quality)
     Fallback:  GLM-4 9B (faster, less VRAM, acceptable quality)
     Emergency: Rule-based transpilation (no LLM, syntax-only)

  OOM Recovery:
     - Pod OOMKill → GKE restarts with 30s delay
     - Reduce max_num_seqs from 8 → 4 automatically
     - Alert ops team to investigate VRAM pressure
```

### 4.4 Model Parallelism Strategies

```
For DeepSeek-R1 67B on 2× A100 80GB:

  Tensor Parallelism (TP=2):
    GPU 0: First half of each transformer layer's weights
    GPU 1: Second half of each transformer layer's weights
    
    Every forward pass: both GPUs compute in parallel
    Communication: All-reduce after each layer (~1ms overhead)
    
    Pros: Low latency (both GPUs active for every token)
    Cons: High inter-GPU bandwidth needed (NVLink)

  Pipeline Parallelism (PP=2):
    GPU 0: Layers 0-32 (first half of model)
    GPU 1: Layers 33-64 (second half of model)
    
    Forward pass: GPU 0 computes → sends activations → GPU 1 computes
    
    Pros: Lower communication overhead
    Cons: Pipeline bubbles (GPU 0 idle while GPU 1 processes)

  For vLLM serving: Tensor Parallelism (TP=2) is preferred
    → Both GPUs active for every request
    → Lower per-request latency
    → NVLink on A100 provides 600 GB/s bandwidth (sufficient)
```

---

## 5. Deep Dive: RAG Pipeline for Migration

### End-to-End Query Flow

```
User: "Convert this Oracle stored procedure to Snowflake"
Input: proc_calculate_tax.sql (150 lines, Oracle PL/SQL)

RAG Pipeline:
  1. Parse input: Extract procedure name, parameters, dependencies
  
  2. Search for similar patterns:
     a. Elasticsearch: "CONNECT BY" → 3 results (exact syntax matches)
     b. Qdrant: embed(proc_calculate_tax) → 5 similar procedures
     c. Hybrid rank (RRF) → Top 5 most relevant examples
  
  3. Build LLM prompt:
     SYSTEM: You are an expert SQL migration assistant. Convert Oracle
     PL/SQL to Snowflake SQL. Use the following reference examples.
     
     CONTEXT (from RAG):
     --- Example 1: Oracle tax calculation → Snowflake equivalent ---
     [retrieved example pair]
     --- Example 2: Oracle cursor pattern → Snowflake RESULTSET ---
     [retrieved example pair]
     
     USER:
     Convert the following Oracle procedure to Snowflake:
     [proc_calculate_tax.sql contents]
  
  4. LLM generates Snowflake SQL (DeepSeek-R1 via vLLM)
  
  5. Post-processing:
     a. Syntax validation (quick parse)
     b. Confidence scoring (how similar to known-good conversions)
     c. Return to user with conversion + explanation + warnings
```

---

## 6. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **GPU node OOM** | LLM serving crashes | Dynamic batching limits; fallback to smaller model; auto-restart |
| **Qdrant index corruption** | Semantic search returns garbage | Persistent volume snapshots; re-index from source files |
| **Elasticsearch query timeout** | Exact search fails | Index optimization; circuit breaker → fall back to vector-only |
| **Model hallucination** | Incorrect SQL generated | Validation layer; confidence scoring; human review for low-confidence |
| **Embedding model mismatch** | Search quality degrades | Version-pin embedding model; re-index on model change |

---

## 7. Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Dual search (ES + Vector) | Vector-only | ES catches exact syntax; vectors miss specific keywords |
| vLLM over Triton | NVIDIA Triton Inference Server | vLLM: purpose-built for LLMs, PagedAttention, continuous batching |
| DeepSeek-R1 67B (Q4) | Llama 3 70B | DeepSeek-R1 excels at reasoning/code; Q4 fits in 2× A100 |
| AST-aware chunking | Fixed-size chunks | Functions are semantic units; splitting mid-function degrades search |
| On-premise over cloud | Cloud API (Anthropic) | Data sovereignty requirement; no data leaves client network |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "67B model on 2 GPUs — isn't that expensive?" | "A100 GPU VMs are ~$3/hr on GKE. For a migration project running 3 months, the AI system runs ~8 hrs/day: $3 × 2 GPUs × 8hrs × 90 days = $4,320. Compare to the manual effort saved — one SQL developer at $150/hr for the same work: $150 × 8hrs × 90 = $108K. The AI system is 25x cheaper even with GPU costs." |
| "What about model quality vs. GPT-4?" | "DeepSeek-R1 67B benchmarks within 5% of GPT-4 on code tasks. For SQL transpilation specifically, the RAG context (few-shot examples from the same codebase) matters more than the base model's capability. A good retrieval system with a decent model outperforms a great model with no context." |
| "How do you handle the cold start?" | "Model weights are pre-loaded on the GPU node's PVC. First inference after pod restart takes ~60 seconds (model loading). After that, continuous batching with vLLM serves requests in 1-3 seconds. For critical deployments, we keep the pod warm with a liveness probe that runs a dummy inference every 5 minutes." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **fully local RAG system** on GKE with three components: **Elasticsearch** for exact keyword/syntax matching, **Qdrant** for semantic code search via embeddings, and **vLLM serving DeepSeek-R1 67B** (4-bit quantized on 2× A100 GPUs with tensor parallelism). The codebase is indexed using AST-aware chunking — splitting at function boundaries, not arbitrary token counts. Queries use **hybrid search with Reciprocal Rank Fusion** — combining exact matches and semantic similarity for the best retrieval. The LLM generates transpilation suggestions with RAG context (similar conversion examples from the same codebase). OOM is managed via dynamic batching limits, context window budgeting, and a fallback chain (67B → 9B → rule-based). All data stays within the client's GKE cluster — zero external API calls."

---

## 10. Key Terms to Drop Naturally

- **RAG (Retrieval-Augmented Generation)**
- **Hybrid search**, **Reciprocal Rank Fusion (RRF)**
- **Elasticsearch**, **Qdrant/Milvus**
- **vLLM**, **PagedAttention**, **continuous batching**
- **Tensor parallelism**, **pipeline parallelism**
- **AST-aware chunking**
- **AWQ quantization** (4-bit)
- **DeepSeek-R1**, **GLM-4**, **open-weight model**
- **Embedding model** (CodeBERT, StarCoder)
- **Data sovereignty**, **air-gapped deployment**
