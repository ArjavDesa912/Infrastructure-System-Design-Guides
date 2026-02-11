# Design a Dead Letter Queue

> **Interview Prompt:** "How do you handle SQL scripts that crash the parser? Design a system for messages that can't be processed."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What makes a message "unprocessable"? (poison messages, schema issues, bugs?) | Determines DLQ routing rules |
| 2 | Should DLQ messages be retried automatically or require human review? | Automation vs. manual workflow |
| 3 | How long should DLQ messages be retained? | Storage and compliance needs |
| 4 | Do we need alerting when DLQ grows? | Operational visibility |
| 5 | Should the DLQ maintain the original message order? | Ordering constraints |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌──────────────┐     ┌──────────┐
│ Producer  │────▶│  Main Queue   │────▶│ Consumer  │
└──────────┘     └──────────────┘     └──────────┘
                                           │
                                    ┌──────┴──────┐
                                    │ Processing   │
                                    │ Succeeds?    │
                                    └──────┬──────┘
                                    YES    │    NO
                                    │      │      │
                                    ▼      │      ▼
                               ┌────────┐  │  ┌────────────┐
                               │ Commit  │  │  │ Retry      │
                               │ Offset  │  │  │ (max N)    │
                               └────────┘  │  └──────┬─────┘
                                           │         │
                                           │    Still failing?
                                           │         │
                                           │         ▼
                                           │  ┌──────────────┐
                                           │  │  Dead Letter  │
                                           │  │  Queue (DLQ)  │
                                           │  └──────┬───────┘
                                           │         │
                                           │         ▼
                                           │  ┌──────────────┐
                                           │  │  DLQ Dashboard│
                                           │  │  + Alerts     │
                                           │  └──────────────┘
                                           │         │
                                           │    Human reviews
                                           │         │
                                           │         ▼
                                           │  ┌──────────────┐
                                           └──│  Replay to   │
                                              │  Main Queue  │
                                              └──────────────┘
```

---

## 3. Deep-Dive: DLQ Design

### 3.1 When to Route to DLQ

| Failure Type | Example | Action |
|-------------|---------|--------|
| **Transient** | Network timeout, 503 | Retry with exponential backoff |
| **Recoverable** | Rate limited, resource busy | Retry after delay |
| **Poison message** | Invalid SQL, corrupt file | DLQ immediately or after max retries |
| **Bug in consumer** | NullPointerException in parser | DLQ, alert engineering team |
| **Schema mismatch** | Message format changed | DLQ, needs producer fix |

**Retry policy:**
```
Attempt 1: Immediate
Attempt 2: Wait 1 second
Attempt 3: Wait 5 seconds
Attempt 4: Wait 30 seconds
Attempt 5: → DLQ (max retries exceeded)
```

### 3.2 DLQ Message Structure

```json
{
    "dlq_id": "uuid-123",
    "original_message": {
        "topic": "conversion-jobs",
        "partition": 3,
        "offset": 12847,
        "key": "tenant-456",
        "value": { "file_id": "abc", "source_sql": "CREATE PROC..." }
    },
    "failure_metadata": {
        "error_type": "PARSE_ERROR",
        "error_message": "Unexpected token 'GOTO' at line 47",
        "stack_trace": "...",
        "consumer_id": "worker-7",
        "attempt_count": 5,
        "first_failed_at": "2024-02-10T10:00:00Z",
        "last_failed_at": "2024-02-10T10:05:30Z"
    },
    "resolution": {
        "status": "PENDING",   // PENDING | RETRYING | RESOLVED | DISCARDED
        "resolved_by": null,
        "resolved_at": null,
        "notes": null
    }
}
```

### 3.3 DLQ Storage Options

| Storage | Best For | Trade-offs |
|---------|----------|------------|
| **Dedicated Kafka topic** | High volume, need replay | Needs separate consumer, less queryable |
| **SQS DLQ** | AWS-native, simple | 14-day max retention |
| **Database table** | Dashboard-friendly, queryable | Slower for high volume |
| **S3 + metadata DB** | Large messages, long retention | More complex, good for archival |

**Recommended:** Kafka DLQ topic + metadata in Postgres for dashboard queries.

### 3.4 DLQ Processing Workflow

```
1. ALERT:    DLQ depth > threshold → page on-call
2. TRIAGE:   Dashboard shows failed messages grouped by error type
3. DIAGNOSE: Engineer reads error + original message
4. FIX:      
   a. If consumer bug → deploy fix → bulk replay DLQ
   b. If bad data → fix data → selective replay
   c. If truly unprocessable → discard with audit trail
5. REPLAY:   Route messages back to main queue
6. VERIFY:   Confirm messages process successfully
```

---

## 4. Replay Strategies

### Bulk Replay
```
All DLQ messages → Main Queue (in order)
```
Use when: Bug was fixed in consumer, all messages should now succeed.

### Selective Replay
```
Filter DLQ by error_type = 'TIMEOUT' → Main Queue
```
Use when: Only certain failures are recoverable.

### Modified Replay
```
Read DLQ message → Transform/fix → Main Queue
```
Use when: Bad data needs correction before reprocessing.

### Replay Safeguards
- **Rate-limit replays** to avoid overwhelming the main queue
- **Mark replayed messages** to prevent double-replay
- **DLQ-of-the-DLQ:** If replayed messages fail again, they go to a secondary DLQ (or get flagged as permanent failures)

### Automated DLQ Categorization

```
Instead of human review for every message:

  Auto-categorizer reads each DLQ message:
    1. Parse error type and message
    2. Match against known patterns:
       - "connection refused" → TRANSIENT → auto-replay after 5 min
       - "schema validation" → PRODUCER_BUG → alert producer team
       - "NullPointerException" → CONSUMER_BUG → alert consumer team
       - Unknown error → UNKNOWN → human review required
    3. Take automated action based on category

  Result: 80% of DLQ messages handled automatically
          20% require human review

  Over time: ML classifier learns from human resolutions
             to auto-categorize new error types
```

---

## 5. Monitoring & Alerting

| Metric | Alert Threshold | What It Means |
|--------|----------------|---------------|
| DLQ depth | > 100 messages | Something is systematically failing |
| DLQ growth rate | > 10 msg/min | Active incident |
| DLQ age (oldest message) | > 24 hours | Stale unresolved failures |
| DLQ error type distribution | New error type appears | Potential new bug |
| Retry exhaustion rate | > 5% of messages | Main queue has a systemic issue |

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just retry forever?" | "Infinite retries waste resources and can amplify failures. A poison message will fail forever, consuming worker capacity. Better to DLQ after N attempts, alert humans, and free workers for healthy messages." |
| "What if the DLQ grows unbounded?" | "We set retention policies (e.g., 30 days), alert when depth exceeds thresholds, and have a dashboard for triage. We also auto-discard messages older than retention with an audit log." |
| "Can you guarantee no message loss?" | "Yes — messages move from main queue → DLQ atomically. The DLQ is durable (replicated Kafka topic or database with backups). We never delete without explicit human action or policy expiration." |
| "How do you handle cascading DLQ growth?" | "Circuit breaker pattern: if DLQ growth rate exceeds a threshold (e.g., >100 msg/min), pause consumption from the main queue. This prevents the DLQ from growing unboundedly during an incident. Resume once the root cause is fixed." |
| "What about compliance and audit?" | "Every DLQ resolution is logged: who resolved it, when, what action (replay/discard), and why. DLQ messages have configurable retention (30, 60, 90 days) to meet compliance requirements. After retention, messages are archived to cold storage." |

---

## 7. Summary: Your Interview Narrative

> "I'd design the DLQ as a **dedicated Kafka topic paired with a metadata store in Postgres**. When a consumer fails to process a message after N retries with exponential backoff, it publishes the original message plus failure metadata to the DLQ topic. A DLQ processor stores the enriched record in Postgres for dashboard queries. The dashboard groups failures by error type for efficient triage. Operators can bulk-replay, selectively replay, or discard messages. Alerts fire when DLQ depth or growth rate exceeds thresholds. Replay is rate-limited to avoid overwhelming the main pipeline."

---

## 8. Key Terms to Drop Naturally

- **Poison message**, **dead letter queue**
- **Exponential backoff with jitter**
- **Replay**, **reprocessing**
- **Visibility timeout**, **max receive count**
- **Circuit breaker** (stop consuming if DLQ grows too fast)
- **Audit trail**, **message provenance**
- **Auto-categorization**, **pattern matching**
- **Secondary DLQ** (DLQ of the DLQ)
