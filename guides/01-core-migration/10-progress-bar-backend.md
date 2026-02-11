# Design a Progress Bar for a Backend Process

> **Interview Prompt:** "How do you push real-time migration status to the UI when the backend process takes hours?"

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | How granular should progress be? (%, per-file, per-stage?) | Determines update frequency |
| 2 | How many concurrent users watching progress? | WebSocket scaling considerations |
| 3 | Should it survive page refresh? | Stateful vs. stateless reconnection |
| 4 | Do we need real-time (<1s) or near-real-time (~5s)? | Push vs. poll trade-off |
| 5 | Is there a notification when complete? (email, push?) | Final delivery mechanism |

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────┐
│           Backend Workers            │
│  Worker 1 ──┐                        │
│  Worker 2 ──┤── Emit Progress ──────▶│──┐
│  Worker N ──┘    Events              │  │
└─────────────────────────────────────┘  │
                                          │
                                          ▼
                                   ┌──────────────┐
                                   │  Event Bus   │
                                   │  (Redis Pub/ │
                                   │   Sub)       │
                                   └──────┬───────┘
                                          │
                                          ▼
                                   ┌──────────────┐
                                   │  Progress    │
                                   │  Aggregator  │
                                   └──────┬───────┘
                                          │
                               ┌──────────┼──────────┐
                               ▼          ▼          ▼
                        ┌──────────┐ ┌──────────┐ ┌──────────┐
                        │WebSocket │ │  SSE     │ │  REST    │
                        │ Server   │ │ Server   │ │  /poll   │
                        └──────────┘ └──────────┘ └──────────┘
                               │          │          │
                               ▼          ▼          ▼
                        ┌──────────────────────────────────┐
                        │        Browser / UI              │
                        │  ┌──────────────────────────┐    │
                        │  │ Migration: 73% Complete   │    │
                        │  │ ████████████░░░░░  4823/  │    │
                        │  │                    6612   │    │
                        │  │ Elapsed: 3h 42m           │    │
                        │  │ ETA: ~1h 25m              │    │
                        │  └──────────────────────────┘    │
                        └──────────────────────────────────┘
```

---

## 3. Communication Mechanisms

### 3.1 WebSocket (Recommended for Real-Time)

```
Client                          Server
  │                                │
  │──── WS Upgrade ───────────────▶│
  │◀─── 101 Switching Protocols ──│
  │                                │
  │◀─── {"progress": 10, ...} ────│
  │◀─── {"progress": 11, ...} ────│
  │◀─── {"progress": 12, ...} ────│
  │                                │
  │──── ping ─────────────────────▶│
  │◀─── pong ─────────────────────│
```

- ✅ Bi-directional, low latency (<100ms), efficient
- ❌ Stateful connections, harder to scale, firewall issues

### 3.2 Server-Sent Events (SSE) (Simpler Alternative)

```javascript
// Client
const eventSource = new EventSource('/api/jobs/123/progress');
eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    updateProgressBar(data.percentage);
};

// Server sends:
// data: {"job_id":"123","percentage":73,"files_done":4823,"total":6612}
// 
// data: {"job_id":"123","percentage":74,"files_done":4889,"total":6612}
```

- ✅ Simple, auto-reconnect built-in, HTTP-based (no firewall issues)
- ❌ Unidirectional (server→client only), limited to ~6 connections per domain

### 3.3 Short Polling (Simplest, Fallback)

```javascript
setInterval(async () => {
    const response = await fetch('/api/jobs/123/progress');
    const data = await response.json();
    updateProgressBar(data.percentage);
}, 3000);  // every 3 seconds
```

- ✅ Simplest to implement, works everywhere
- ❌ Wastes bandwidth, adds server load, not truly real-time

### Comparison Table

| Mechanism | Latency | Scalability | Complexity | Best For |
|-----------|---------|-------------|------------|----------|
| WebSocket | <100ms | Medium | High | Interactive dashboards |
| SSE | <1s | High | Low | Progress updates (recommended) |
| Short Polling | 1-5s | Low | Lowest | Fallback / simple UIs |

**Recommended:** SSE as primary, short polling as fallback.

---

## 4. Progress Aggregation

Workers process files independently. We need to aggregate into a unified progress view.

```
Worker 1: files 1-2000    → "I've done 1,847 files"
Worker 2: files 2001-4000 → "I've done 1,956 files"
Worker 3: files 4001-6612 → "I've done 1,020 files"

Aggregator combines:
  total_done = 1847 + 1956 + 1020 = 4823
  total = 6612
  percentage = 72.9%
  
  estimated_rate = 4823 / elapsed_seconds
  eta = (6612 - 4823) / estimated_rate
```

### Aggregation strategies:

**Option A: Counter in Redis (simple)**
```python
# Each worker increments atomically
redis.incr(f"job:{job_id}:done")

# Progress service reads
done = redis.get(f"job:{job_id}:done")
total = redis.get(f"job:{job_id}:total")
```

**Option B: Per-worker progress (more detail)**
```python
# Each worker reports its own progress
redis.hset(f"job:{job_id}:progress", worker_id, json.dumps({
    "done": 1847, "assigned": 2000, "failed": 3
}))

# Aggregator reads all workers
all_progress = redis.hgetall(f"job:{job_id}:progress")
total_done = sum(p["done"] for p in all_progress.values())
```

---

## 5. Client-Side Progress UI

```javascript
class ProgressTracker {
    constructor(jobId) {
        this.jobId = jobId;
        this.eventSource = null;
        this.smoothedProgress = 0;
    }

    connect() {
        this.eventSource = new EventSource(`/api/jobs/${this.jobId}/progress`);
        
        this.eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.animateProgress(data.percentage);
            this.updateStats(data);
        };

        this.eventSource.onerror = () => {
            // Auto-reconnect is built into SSE
            // Fallback to polling if SSE fails
            this.fallbackToPolling();
        };
    }

    animateProgress(target) {
        // Smooth animation: don't jump from 30% to 73%
        const animate = () => {
            if (this.smoothedProgress < target) {
                this.smoothedProgress += 0.5;
                this.renderBar(this.smoothedProgress);
                requestAnimationFrame(animate);
            }
        };
        animate();
    }

    updateStats(data) {
        document.getElementById('eta').textContent = 
            `ETA: ${formatDuration(data.eta_seconds)}`;
        document.getElementById('files').textContent = 
            `${data.files_done} / ${data.total_files}`;
        document.getElementById('failed').textContent = 
            `${data.failed_count} failed`;
    }
}
```

---

## 6. ETA Calculation

```python
def calculate_eta(job_id):
    progress = get_progress(job_id)
    
    # Simple: linear extrapolation
    elapsed = now() - progress.started_at
    rate = progress.done / elapsed.total_seconds()
    remaining = progress.total - progress.done
    
    if rate > 0:
        eta_seconds = remaining / rate
    else:
        eta_seconds = None  # can't estimate yet
    
    # Better: exponentially weighted moving average (EWMA)
    # Smooths out rate fluctuations
    alpha = 0.3
    smoothed_rate = alpha * current_rate + (1 - alpha) * previous_smoothed_rate
    eta_seconds = remaining / smoothed_rate
    
    return eta_seconds
```

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if the user closes the browser and comes back?" | "Progress state lives server-side (Redis/DB), not in the WebSocket connection. When the user reconnects, they get the current state immediately. SSE auto-reconnects natively. We also send completion notifications via webhook or email." |
| "WebSockets don't scale" | "For this use case, SSE is sufficient since we only need server→client updates. SSE is just long-lived HTTP — it works through load balancers and CDNs. For high connection counts, we can use a pub/sub fanout (one backend subscription, many SSE connections)." |
| "How do you handle 1000 users watching the same job?" | "Redis Pub/Sub — one subscription per job. The SSE server subscribes to the job's channel and fans out to all connected clients. The backend workers publish progress once, regardless of how many clients are watching." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **three-layer progress system**: (1) Workers atomically increment counters in Redis as they complete files, (2) A progress aggregator combines per-worker progress into a job-level view with percentage, file counts, and ETA, (3) The UI receives real-time updates via **Server-Sent Events (SSE)**. SSE is simpler than WebSockets for this unidirectional use case and has built-in auto-reconnect. For ETA, I'd use an EWMA-smoothed rate calculation to avoid jumpy estimates. The progress bar animates smoothly on the client side, and completion triggers email/webhook notifications."

---

## 9. Key Terms to Drop Naturally

- **Server-Sent Events (SSE)**, **WebSocket**
- **Pub/Sub fan-out** (Redis)
- **EWMA** (Exponentially Weighted Moving Average)
- **Auto-reconnect**, **graceful degradation**
- **Progress aggregation**, **atomic counters**
- **Long polling** (fallback)
