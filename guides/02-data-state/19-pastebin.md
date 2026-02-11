# Design a Pastebin

> **Interview Prompt:** "Design a system for storing code snippets with expiration, like Pastebin."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Expected traffic? (reads/sec, writes/sec) | Caching and scaling strategy |
| 2 | Max snippet size? | Storage and CDN decisions |
| 3 | Do we need user accounts, or anonymous pastes? | Auth complexity |
| 4 | Expiration options? (1 hour, 1 day, never?) | TTL and cleanup design |
| 5 | Do we need syntax highlighting or just raw text? | Frontend complexity |

---

## 2. High-Level Architecture

```
┌──────────┐     ┌──────────────┐     ┌──────────────┐
│  Client   │────▶│  API Service  │────▶│  Metadata DB │
│  (Web UI) │     │  (Stateless) │     │  (Postgres)  │
└──────────┘     └──────┬───────┘     └──────────────┘
                        │
                        ├─▶ Blob Store (S3 — snippet content)
                        │
                        └─▶ Cache (Redis — hot snippets)
                              │
                              ▼
                        ┌──────────────┐
                        │  CDN          │
                        │  (CloudFront) │
                        │  Cache static │
                        │  snippets     │
                        └──────────────┘
```

### Core Components

1. **API Service** — Handles create/read/delete operations
2. **URL Shortener** — Generates unique short IDs for snippets
3. **Metadata DB** — Stores snippet metadata (title, language, expiry, owner)
4. **Blob Store** — Stores raw content (S3 for durability, cheap storage)
5. **Cache** — Redis for frequently accessed snippets
6. **Cleanup Service** — Deletes expired snippets

---

## 3. Deep-Dive: Core Design

### 3.1 URL/ID Generation

```
Requirement: Short, unique, unguessable URLs
Example: https://paste.example.com/aB3kX9

Options:
  1. Base62 encoding of auto-increment ID
     ID: 1000000 → Base62: "4c92" (4 chars)
     ✅ Short, unique
     ❌ Sequential = guessable

  2. Random Base62 string (8 chars)
     "aB3kX9pQ" → 62^8 = 218 trillion combinations
     ✅ Unguessable, short
     ❌ Collision possible (check before insert)

  3. Hash-based (first 8 chars of SHA-256)
     SHA-256(content + timestamp) → "a1b2c3d4"
     ✅ Deterministic, collision-resistant
     
Recommended: Random Base62 (8 chars) with collision check
```

### 3.2 Write Path

```
1. Client sends: POST /api/paste
   { "content": "SELECT * FROM...", "lang": "sql", "expires_in": "1h" }

2. Generate unique ID: "aB3kX9pQ"

3. Store content in S3: s3://pastes/aB/3k/X9pQ.txt
   (directory structure from ID for even distribution)

4. Store metadata in Postgres:
   INSERT INTO pastes (id, lang, expires_at, created_at, size_bytes)

5. Optionally cache in Redis: SET paste:aB3kX9pQ <content> EX 3600

6. Return: { "url": "https://paste.example.com/aB3kX9pQ" }
```

### 3.3 Read Path

```
1. Client requests: GET /aB3kX9pQ

2. Check CDN cache → HIT? Return ✅

3. Check Redis cache → HIT? Return ✅

4. Check Postgres metadata:
   → Expired? Return 410 Gone
   → Not found? Return 404

5. Fetch content from S3

6. Cache in Redis + CDN

7. Return content with syntax highlighting
```

### 3.4 Expiration & Cleanup

```
Cleanup Service (runs every 5 minutes):

1. Query: SELECT id, content_ref FROM pastes
          WHERE expires_at < NOW()
          LIMIT 1000

2. For each expired paste:
   a. Delete from S3
   b. Delete from Redis cache
   c. Delete metadata from Postgres

3. CDN invalidation for expired URLs
   (or rely on CDN TTL being shorter than paste TTL)
```

### 3.5 Content Deduplication

```
If two users paste the same content:

Option A: No dedup (simple)
  Paste 1: s3://pastes/aB/3k/X9pQ.txt → "SELECT * FROM users"
  Paste 2: s3://pastes/cD/5m/W7rS.txt → "SELECT * FROM users" (duplicate!)

Option B: Content-addressable (storage efficient)
  content_hash = SHA-256("SELECT * FROM users") = "e5f6..."
  Store: s3://pastes/e5/f6/.../content.txt
  Both paste IDs point to the same S3 object
  Reference counting: delete S3 object only when last reference is removed

Trade-off:
  Dedup saves ~20-30% storage in practice
  But adds complexity (reference counting, race conditions on delete)
  Recommended: skip dedup unless storage costs are a concern
```

### 3.6 Syntax Highlighting

```
Two approaches:

Client-side (recommended):
  - Server returns raw text + language hint
  - Browser uses highlight.js or Prism to render
  - ✅ No server compute cost
  - ✅ CDN can cache raw text (one version)

Server-side:
  - Server generates HTML with <span class="keyword">SELECT</span>
  - ✅ Works without JavaScript
  - ❌ More compute, more storage (HTML > raw text)
  - ❌ CDN cache per language is wasteful
```

---

## 4. Data Model

```sql
CREATE TABLE pastes (
    id              VARCHAR(8) PRIMARY KEY,
    title           VARCHAR(255),
    language        VARCHAR(50),
    size_bytes      INT,
    visibility      ENUM('PUBLIC','UNLISTED','PRIVATE'),
    user_id         UUID,            -- null for anonymous
    expires_at      TIMESTAMP,       -- null for never
    view_count      INT DEFAULT 0,
    content_ref     TEXT,            -- S3 URI
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_pastes_expiry ON pastes(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_pastes_user ON pastes(user_id) WHERE user_id IS NOT NULL;
```

---

## 5. API Design

```
POST /api/paste
  Body: { "content": "...", "title": "optional", "language": "sql", 
          "visibility": "unlisted", "expires_in": "1h" }
  Auth: optional (API key for account-linked pastes)
  Response: 201 { "id": "aB3kX9pQ", "url": "https://paste.example.com/aB3kX9pQ" }

GET /api/paste/{id}
  Response: 200 { "content": "...", "language": "sql", "created_at": "...", 
                   "expires_at": "...", "view_count": 42 }
  Error: 404 (not found), 410 (expired)

GET /api/paste/{id}/raw
  Response: 200 text/plain (raw content, no JSON wrapper)

DELETE /api/paste/{id}
  Auth: required (owner only)
  Response: 204 No Content

GET /api/user/{user_id}/pastes?page=1&limit=20
  Auth: required
  Response: 200 { "pastes": [...], "total": 156 }
```

---

## 5. Capacity Estimation

```
Assumptions:
  - 5M pastes/month created
  - Average snippet: 5KB
  - Read:Write ratio: 10:1
  - 80% expire within 24 hours

Storage:
  Monthly: 5M × 5KB = 25GB
  Active (unexpired): ~5GB at any time
  Yearly: 300GB (mostly expired, cleaned up)

Traffic:
  Writes: 5M / 30 / 86400 ≈ 2 writes/sec (easy)
  Reads:  20 reads/sec peak (cacheable)

Redis cache: 1GB (top 10K hot pastes)
```

---

## 7. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Hot pastes** (viral snippet) | CDN absorbs 99% of reads; Redis handles the rest |
| **Write spikes** (bot abuse) | Rate limiting per IP (10/min anonymous, 60/min authenticated) |
| **S3 costs at scale** | Deduplication via content hashing; lifecycle policies for auto-archival |
| **Cleanup lag** (millions of expired) | Batch deletion with rate limiting; partition expiry index by date |
| **Large pastes** (someone uploads 10MB) | Max paste size limit (512KB default); reject with 413 |
| **Search across pastes** | Elasticsearch index for public pastes; full-text search by content |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why S3 and not just the database?" | "Storing blob content in the database increases DB size and backup times. S3 is cheaper for blob storage ($0.023/GB/month), infinitely scalable, and we can serve content directly through CDN. The database only stores tiny metadata rows." |
| "How do you handle abuse?" (spam, malware) | "Rate limiting per IP/user (max 10 pastes/minute). Content scanning for known malware signatures. Report/flag mechanism. CAPTCHAs for anonymous pastes. IP-based abuse detection." |
| "What about privacy for private pastes?" | "Private pastes are encrypted at rest with a key derived from the URL. Without the URL, the content is unreadable. We don't index private pastes and apply URL randomization (long keys) to make them unguessable." |
| "What if a viral paste gets millions of views?" | "CDN handles the spike. Hot content is cached at edge locations worldwide. Even if a paste goes viral, our origin servers see minimal traffic — CDN serves 99%+ of requests from cache." |
| "How do you handle paste versioning or editing?" | "Two approaches: (1) Immutable pastes — create a new paste for each edit, link them via a 'revised_from' field. (2) Mutable with version history — store each version as a separate S3 object, metadata tracks version list. I'd start with immutable for simplicity." |

---

## 9. Summary: Your Interview Narrative

> "I'd design Pastebin as a **three-tier system**: a stateless API service, Postgres for metadata, and S3 for content storage. Snippet IDs are 8-character random Base62 strings, giving 218 trillion combinations. Reads are optimized with a multi-layer cache: CDN for static snippets, Redis for hot snippets, S3 as the durable store. A cleanup service runs periodically to delete expired pastes from all layers. The system handles 5M pastes/month with 25GB/month storage, easily scaling horizontally since the API is stateless and the heavy lifting is done by S3 and CDN."

---

## 10. Key Terms to Drop Naturally

- **Base62 encoding**, **URL shortening**
- **CDN** (Content Delivery Network)
- **TTL-based expiration**, **lazy cleanup**
- **Content-addressable** (deduplication option)
- **Rate limiting**, **abuse prevention**
- **Cache-aside pattern** (Redis + S3)
- **Multi-layer caching** (CDN → Redis → S3)
- **413 Payload Too Large** (size limits)
