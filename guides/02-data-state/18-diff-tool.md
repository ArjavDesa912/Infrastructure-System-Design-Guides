# Design a "Diff" Tool

> **Interview Prompt:** "Design a tool that shows users what changed between Oracle SQL and Snowflake SQL."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Line-by-line diff or semantic diff (by SQL structure)? | Algorithm and complexity |
| 2 | What formats? (side-by-side, unified, inline?) | Rendering strategy |
| 3 | Do we need to diff across languages/dialects? | Semantic normalization |
| 4 | Expected file sizes? (10 lines vs. 10K lines?) | Performance constraints |
| 5 | Interactive (real-time in browser) or batch output? | Architecture approach |

---

## 2. High-Level Architecture

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Input    │───▶│  Normalize   │───▶│  Diff Engine │───▶│  Render      │
│  Files    │    │  & Parse     │    │  (LCS/Myers) │    │  (HTML/API)  │
│  (A & B)  │    │              │    │              │    │              │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                                                                │
                                                                ▼
                                                        ┌──────────────┐
                                                        │  UI Display  │
                                                        │  Side-by-side│
                                                        │  + Annotations│
                                                        └──────────────┘
```

---

## 3. Deep-Dive: Diff Algorithms

### 3.1 Longest Common Subsequence (LCS)

The foundation of most diff tools.

```
File A: ["CREATE TABLE", "id INT", "name VARCHAR", "age INT"]
File B: ["CREATE TABLE", "id NUMBER", "name VARCHAR(100)", "age INT", "email VARCHAR"]

LCS: ["CREATE TABLE", "age INT"]

Diff output:
  = CREATE TABLE       (unchanged)
  - id INT             (deleted from A)
  + id NUMBER          (added in B)
  - name VARCHAR       (deleted from A)
  + name VARCHAR(100)  (added in B)
  = age INT            (unchanged)
  + email VARCHAR      (added in B)
```

**Time complexity:** O(N × M) where N, M are line counts
**Space optimization:** Hirschberg's algorithm → O(min(N, M)) space

### 3.2 Myers' Diff Algorithm (Git's Default)

```
Finds the shortest edit script (minimum number of insertions + deletions).

Operates on an edit graph:
  X-axis: lines in file A
  Y-axis: lines in file B
  Diagonal: matching lines (free moves)
  Horizontal: delete from A
  Vertical: insert from B

Finds shortest path from (0,0) to (N,M) → optimal diff
```

- ✅ Produces minimal diffs
- ✅ O((N+M)×D) where D = number of differences (fast when files are similar)
- Git, GNU diff, and most tools use this

### 3.3 Semantic Diff (SQL-Aware)

Instead of diffing raw text lines, diff parsed SQL structures:

```
Oracle SQL (AST):
  TABLE: "users"
    COLUMN: "id" TYPE: "NUMBER(10)"
    COLUMN: "name" TYPE: "VARCHAR2(100)"
    COLUMN: "created_at" TYPE: "DATE"

Snowflake SQL (AST):
  TABLE: "users"
    COLUMN: "id" TYPE: "NUMBER(10)"
    COLUMN: "name" TYPE: "VARCHAR(100)"
    COLUMN: "created_at" TYPE: "TIMESTAMP_NTZ"

Semantic Diff:
  TABLE "users": unchanged
    COLUMN "id": unchanged
    COLUMN "name": type changed VARCHAR2(100) → VARCHAR(100)
    COLUMN "created_at": type changed DATE → TIMESTAMP_NTZ
```

- ✅ Understands SQL structure, not just text
- ✅ Ignores whitespace, formatting changes
- ❌ Requires parser for each dialect

---

## 4. Diff Rendering Formats

### Unified Diff
```diff
--- a/procedure.sql (Oracle)
+++ b/procedure.sql (Snowflake)
@@ -1,5 +1,5 @@
 CREATE TABLE users (
-    id NUMBER(10),
+    id NUMBER(10,0),
-    name VARCHAR2(100),
+    name VARCHAR(100),
     email VARCHAR(255)
 );
```

### Side-by-Side
```
Oracle (Source)               │ Snowflake (Target)
─────────────────────────────│──────────────────────────────
CREATE TABLE users (          │ CREATE TABLE users (
  id NUMBER(10),         [M]  │   id NUMBER(10,0),
  name VARCHAR2(100),    [M]  │   name VARCHAR(100),
  email VARCHAR(255)          │   email VARCHAR(255)
);                            │ );
                         [A]  │   created_at TIMESTAMP_NTZ
```

### Inline (Annotated)
```sql
CREATE TABLE users (
    id NUMBER(10),       -- ⚠️ CHANGED: Oracle NUMBER(10) → Snowflake NUMBER(10,0)
    name VARCHAR(100),   -- ⚠️ CHANGED: Oracle VARCHAR2(100) → Snowflake VARCHAR(100)
    email VARCHAR(255)   -- ✅ UNCHANGED
);
-- ➕ ADDED: created_at TIMESTAMP_NTZ
```

---

## 5. Architecture for Scale

```
For comparing entire migration projects (50K+ files):

┌─────────────────────────┐
│  Diff Job Orchestrator   │
│  Input: project_A_ref,   │
│         project_B_ref    │
└──────────┬──────────────┘
           │ Fan-out by file
     ┌─────┼─────┐
     ▼     ▼     ▼
 ┌──────┐┌──────┐┌──────┐
 │Diff  ││Diff  ││Diff  │      Per-file diff workers
 │Worker││Worker││Worker│
 └──┬───┘└──┬───┘└──┬───┘
    │       │       │
    ▼       ▼       ▼
┌──────────────────────────┐
│   Diff Report Aggregator  │
│   - Summary statistics    │
│   - Per-file diffs        │
│   - Category breakdown    │
└──────────────────────────┘

### Word-Level Diff (Within Modified Lines)

```
Line-level diff says: "line changed"
Word-level diff says: "which words changed"

Example:
  Line A: "DECLARE @count INT = 0;"
  Line B: "LET count INT := 0;"

  Word diff:
    - DECLARE @count
    + LET count
    = INT
    - =
    + :=
    = 0;

Algorithm: apply Myers' diff at word/token level within modified line pairs.
UI: highlight changed words in red/green within the line.

This gives users surgical precision on what exactly changed,
not just "this line is different."
```

### Performance Optimization

```
For 50K+ file projects:

  Phase 1: Hash comparison (instant)
    hash(file_A) == hash(file_B)? → UNCHANGED (skip diff)
    ~90% of files are unchanged → saves 90% of work

  Phase 2: Size-based estimation
    If file > 100KB → use patience diff (better for large files)
    If file < 1KB → inline diff in browser (no server round-trip)

  Phase 3: Lazy diff
    Compute detailed diff only when user clicks on a file
    Cache computed diffs for subsequent views

  Phase 4: Streaming
    For very large files, stream diff results line by line
    Show partial results immediately while computing remainder
```
```

---

## 6. Diff Report Structure

```json
{
    "summary": {
        "total_files": 1200,
        "unchanged": 0,
        "modified": 1150,
        "added": 50,
        "deleted": 0,
        "change_categories": {
            "data_type_changes": 892,
            "syntax_rewrites": 423,
            "function_replacements": 187,
            "structural_changes": 48
        }
    },
    "files": [
        {
            "path": "procedures/sp_get_users.sql",
            "status": "modified",
            "changes": 12,
            "hunks": [
                {
                    "source_line": 5,
                    "target_line": 5,
                    "type": "modification",
                    "source": "DECLARE @count INT",
                    "target": "LET count INT := 0;",
                    "category": "syntax_rewrite"
                }
            ]
        }
    ]
}
```

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Line-based diff misses semantic changes" | "I'd offer two modes: (1) text diff for raw comparison, (2) semantic diff that parses both SQL dialects and compares ASTs. The semantic diff categorizes changes (data type, syntax, function) for better understanding." |
| "How do you handle 50K files?" | "Fan-out to parallel diff workers. Each worker handles one file pair. Results are aggregated into a summary report. Diffs are computed lazily — we generate the overview first, then compute detailed per-file diffs on demand when the user clicks in." |
| "What about binary files?" | "Detect binary files by checking for null bytes in the first 8KB. Binary files are marked as 'changed' or 'unchanged' (by hash comparison) without showing content diff." |
| "How do you handle move detection?" | "After computing adds and deletes, compare deleted files to added files by content hash. If a deleted file has >80% similarity to an added file (using fuzzy matching), flag it as a rename/move rather than delete+add. Git uses this same heuristic." |
| "What if diffing is too slow for interactive use?" | "Three strategies: (1) hash comparison to skip unchanged files instantly, (2) lazy diff — only compute when user clicks on a file, (3) cache computed diffs with invalidation on source change. For real-time editing, debounce diff computation to run at most every 500ms." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **two-mode diff tool**: text diff using Myers' algorithm for raw line-by-line comparison, and semantic diff that parses SQL into ASTs and compares structurally. For migration projects with thousands of files, diffs are computed in parallel by worker pools and aggregated into a summary report with categories (data type changes, syntax rewrites, etc.). The UI offers side-by-side and unified views with syntax highlighting. For performance, I compute the file list diff first (which files changed, based on hash comparison), then generate detailed line diffs on demand."

---

## 9. Key Terms to Drop Naturally

- **Myers' algorithm**, **LCS** (Longest Common Subsequence)
- **Edit distance**, **edit script**
- **Hunk** (a block of contiguous changes)
- **AST diff**, **semantic diff**
- **Unified diff**, **side-by-side diff**
- **Content hash** (fast unchanged detection)
- **Word-level diff**, **token-level diff**
- **Patience diff** (for large files)
- **Move/rename detection**
