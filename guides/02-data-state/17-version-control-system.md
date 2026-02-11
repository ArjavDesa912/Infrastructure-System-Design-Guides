# Design a Version Control System (Git-Lite)

> **Interview Prompt:** "Design a system like Git for storing versions of converted code."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Do we need branching and merging, or just linear history? | Complexity of the DAG model |
| 2 | What's the expected repository size? | Storage optimization strategy |
| 3 | Centralized (like SVN) or distributed (like Git)? | Architecture decision |
| 4 | Do we need conflict resolution for concurrent edits? | Merge strategy design |
| 5 | How many versions per file? (dozens vs. thousands?) | Retention and storage costs |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────┐
│                    Object Store                       │
│                                                       │
│  ┌──────┐     ┌──────┐     ┌──────┐                 │
│  │ Blob │     │ Tree │     │Commit│                  │
│  │(file │     │(dir  │     │(snap │                  │
│  │ data)│     │ list)│     │ shot)│                  │
│  └──────┘     └──────┘     └──────┘                  │
│                                                       │
│  Every object → SHA-1 hash → content-addressable      │
└──────────────────────────────────────────────────────┘
          │              │              │
          ▼              ▼              ▼
┌──────────────────────────────────────────────────────┐
│                  Reference Store                      │
│  main   → commit abc123                               │
│  dev    → commit def456                               │
│  HEAD   → ref: refs/heads/main                       │
│  tags/v1.0 → commit 789abc                           │
└──────────────────────────────────────────────────────┘
```

### Core Data Model (Git's Actual Design)

**Three object types, all content-addressed by SHA-1:**

1. **Blob** — Raw file content (no filename, just bytes)
2. **Tree** — Directory listing: maps filenames → blob hashes (or sub-tree hashes)
3. **Commit** — Points to a tree (snapshot), parent commit(s), author, message, timestamp

```
Commit: abc123
  ├── tree: def456
  │     ├── "README.md" → blob: 111aaa
  │     ├── "src/" → tree: 222bbb
  │     │     ├── "main.sql" → blob: 333ccc
  │     │     └── "utils.sql" → blob: 444ddd
  │     └── "converted/" → tree: 555eee
  │           ├── "main.sql" → blob: 666fff  (converted version)
  │           └── "utils.sql" → blob: 777ggg
  ├── parent: prev-commit-hash
  ├── author: "Alice <alice@example.com>"
  ├── message: "Convert stored procedures to Snowflake"
  └── timestamp: 2024-02-10T10:30:00Z
```

---

## 3. Deep-Dive: Core Design

### 3.1 Content-Addressable Storage

```
Every object is stored by its hash:

blob_content = "CREATE PROCEDURE foo AS..."
hash = SHA-1(blob_content) = "a1b2c3d4..."

Stored at: .vcs/objects/a1/b2c3d4...

Benefits:
  - Same content = same hash → automatic deduplication
  - Integrity checking: hash the content, compare to filename
  - Immutable: changing content changes the hash → new object
```

### 3.2 How Commits Chain Together

```
Commit C3 (latest)
  parent → Commit C2
              parent → Commit C1
                          parent → null (initial commit)

Each commit is a FULL SNAPSHOT (tree), not a diff.
But storage is efficient because unchanged blobs are shared:

C1 tree: { "file_a.sql" → blob_X, "file_b.sql" → blob_Y }
C2 tree: { "file_a.sql" → blob_X, "file_b.sql" → blob_Z }  
                           ↑ same blob (unchanged)    ↑ new blob (modified)
```

### 3.3 Branching

```
Branches are just pointers to commits:

main → C3
dev  → C5

    C1 ← C2 ← C3 (main)
              ↖
               C4 ← C5 (dev)

Creating a branch = creating a new pointer (one write operation)
```

### 3.4 Diff Generation

```
Compare two trees (commit A vs. commit B):

Tree A: { "file1" → blob_X, "file2" → blob_Y, "file3" → blob_Z }
Tree B: { "file1" → blob_X, "file2" → blob_W, "file4" → blob_V }

Diff:
  file1: unchanged (same blob hash)
  file2: MODIFIED (blob_Y → blob_W)
  file3: DELETED
  file4: ADDED

For modified files: compute line-by-line diff (LCS algorithm)
```

### 3.5 Merge Strategy

```
Three-Way Merge:

Base (common ancestor):  line 1, line 2, line 3, line 4
Branch A:                line 1, line 2a, line 3, line 4
Branch B:                line 1, line 2, line 3, line 4b

Result:                  line 1, line 2a, line 3, line 4b

Conflict (both changed same line):
Base:      line 2
Branch A:  line 2a
Branch B:  line 2b
→ CONFLICT: human must resolve
```

---

## 4. Storage Optimizations

| Optimization | How It Works |
|-------------|-------------|
| **Deduplication** | Same content = same hash = stored once |
| **Packfiles** | Compress multiple objects into a single file with delta encoding |
| **Delta encoding** | Store diffs between similar objects (not full copies) |
| **Shallow clones** | Client requests only recent N commits (not full history) |
| **Garbage collection** | Remove unreferenced objects (orphaned by branch deletion) |

### Packfile Delta Encoding
```
Before packing:
  blob_v1: 500 KB (full content)
  blob_v2: 500 KB (full content, 95% identical to v1)
  Total: 1 MB

After packing:
  blob_v1: 500 KB (base object)
  blob_v2: 25 KB (delta: "add line 47, delete line 52")
  Total: 525 KB (48% smaller)
```

---

## 5. API Design

```
POST   /repos/{repo_id}/commits     — Create a new commit (snapshot)
GET    /repos/{repo_id}/commits     — List commit history
GET    /repos/{repo_id}/tree/{ref}  — Get directory tree at a ref (commit/branch)
GET    /repos/{repo_id}/blob/{hash} — Get file content by hash

POST   /repos/{repo_id}/branches    — Create a branch
GET    /repos/{repo_id}/diff?from=A&to=B  — Get diff between two refs

POST   /repos/{repo_id}/merge       — Merge two branches
```

### Capacity Estimation

```
Assumptions:
  10K repositories, 1000 commits avg per repo
  Average commit: changes 5 files, each ~10KB
  Deduplication ratio: 90% (most files unchanged per commit)

Storage (objects):
  10K repos × 1000 commits × 5 changed blobs × 10KB = 500GB raw changes
  With dedup: ~50GB unique blobs
  Packfiles with delta compression: ~15GB

Metadata:
  10K repos × 1000 commits = 10M commit objects (~200 bytes each = 2GB)
  10M tree objects (~500 bytes each = 5GB)

Total: ~22GB with packfiles (very efficient!)

Operations:
  Clone (shallow): fetch latest commit + tree + blobs ~50ms
  Commit: write 5 blobs + 1 tree + 1 commit ~10ms
  Diff: compare 2 trees, fetch changed blobs ~20ms
```

---

## 6. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Storing full snapshots per commit is wasteful" | "It's actually efficient due to content-addressable storage. Unchanged files share the same blob object — they're not duplicated. Additionally, packfiles use delta compression for even better storage efficiency. Git stores entire Linux kernel history in a few GB." |
| "Why SHA-1 and not something else?" | "SHA-1 gives a deterministic, collision-resistant hash that serves as both a content address and integrity check. For newer systems, we could use SHA-256 for better collision resistance. The key property is that identical content produces identical hashes." |
| "How do you handle large binary files?" | "Large binaries (images, datasets) are handled separately: store them in a blob store (S3) and track references via a pointer file (similar to Git LFS). This keeps the main object store lean." |
| "How do you handle concurrent pushes?" | "Optimistic locking: a push includes the expected parent commit hash. If another push happened first, the parent hash won't match the branch tip → reject with 'non-fast-forward' error. The pusher must pull, merge, and push again. This is how Git works." |
| "Why not store diffs instead of snapshots?" | "Snapshots make checkout O(1) — you just read the tree at that commit. With diff-based storage, checkout requires replaying all diffs from the beginning (O(n)). Snapshots with content deduplication give us the best of both worlds: fast access and efficient storage." |

---

## 7. Summary: Your Interview Narrative

> "I'd design a **content-addressable version control system** inspired by Git. Every file, directory, and commit is stored as an object identified by its SHA-1 hash. Blobs store file content, trees store directory listings pointing to blobs, and commits point to trees with metadata (author, message, parent). Branches are lightweight pointers to commits. Since objects are content-addressed, identical files are automatically deduplicated — only changed files create new blobs. Diffs are computed by comparing tree objects. Merging uses three-way merge with the common ancestor. Storage is further optimized with packfiles and delta compression."

---

## 8. Key Terms to Drop Naturally

- **Content-addressable storage**, **SHA-1 hash**
- **Blob**, **tree**, **commit** (Git object types)
- **DAG** (Directed Acyclic Graph of commits)
- **Three-way merge**, **conflict resolution**
- **Delta compression**, **packfile**
- **Shallow clone**, **garbage collection**
- **Optimistic locking**, **fast-forward merge**
- **Git LFS** (large file storage)
