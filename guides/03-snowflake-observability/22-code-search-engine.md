# Design a Code Search Engine

> **Interview Prompt:** "Design a system that lets users search through millions of source files — grep at scale."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | Exact string search or regex? | Index structure and query complexity |
| 2 | How much code? (millions of files, repos?) | Scale of indexing |
| 3 | How fresh? (real-time updates or batch re-index?) | Index update strategy |
| 4 | Do we need ranking/relevance or just find-all? | Search algorithm |
| 5 | Search within specific repos/languages? | Scope filtering |

---

## 2. High-Level Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│  Code Repos   │───▶│  Indexer     │───▶│  Search Index    │
│  (Git, S3)    │    │  Pipeline    │    │  (Inverted Index │
│               │    │              │    │   + Trigram)      │
└──────────────┘    └──────────────┘    └──────────────────┘
                                                │
                                                ▼
┌──────────────┐                        ┌──────────────────┐
│  User        │───── Search Query ────▶│  Query Engine     │
│  (Web UI)    │◀──── Results ─────────│  (Parse + Route)  │
└──────────────┘                        └──────────────────┘
```

---

## 3. Deep-Dive: Indexing Strategies

### 3.1 Trigram Index (Primary Strategy for Code Search)

```
How trigrams work:
  Word: "SELECT" → Trigrams: ["SEL", "ELE", "LEC", "ECT"]

Index (inverted):
  "SEL" → [file1:L3, file7:L42, file9:L1, ...]
  "ELE" → [file1:L3, file5:L10, ...]
  "LEC" → [file1:L3, file7:L42, ...]
  "ECT" → [file1:L3, file7:L42, file12:L8, ...]

Query: "SELECT"
  1. Look up trigrams: "SEL" ∩ "ELE" ∩ "LEC" ∩ "ECT"
  2. Intersection → candidate files: [file1, file7]
  3. Verify candidates with actual grep (eliminate false positives)
  4. Return confirmed matches with line numbers
```

**Why trigrams for code?**
- Code search needs **substring matching** (find "proto" in "prototype")
- Standard word-based indexes only match whole words
- Trigrams handle arbitrary substrings efficiently

### 3.2 Inverted Index (for Full Tokens)

```
Token: "CREATE" → [file1:L1, file3:L5, file7:L12]
Token: "TABLE"  → [file1:L1, file3:L5, file8:L3]

Query: "CREATE TABLE"
  → "CREATE" ∩ "TABLE" → [file1:L1, file3:L5]
  → Positional check: are they adjacent?
  → file1:L1 ✅ (both on same line), file3:L5 ✅
```

### 3.3 Hybrid Approach (Recommended)

```
Query arrives:
  ├── Looks like exact phrase? → Trigram index
  ├── Looks like keyword search? → Inverted index  
  ├── Regex pattern? → Trigram pre-filter → regex verify
  └── Symbol search? → Symbol table (language-aware index)
```

---

## 4. Indexing Pipeline

```
1. Crawl:
   - Walk all repos/directories
   - Track file changes (git diff since last index)
   - Skip binary files, node_modules, build artifacts

2. Parse:
   - Detect language (by extension + content)
   - Tokenize: split into words, symbols, operators
   - Generate trigrams
   - Extract symbols (functions, classes, variables)

3. Build Index:
   - Inverted index: token → [(file, line, position)]
   - Trigram index: trigram → [file_ids]  
   - Symbol table: symbol_name → [(file, line, kind)]

4. Store:
   - Sharded across search nodes
   - Each shard handles a subset of files
   - Replicated for fault tolerance
```

### Incremental Indexing
```
Full re-index: expensive (hours for millions of files)
Incremental: only re-index changed files

Git-based tracking:
  last_indexed_commit: abc123
  current_commit: def456
  
  Changed files = git diff abc123..def456 --name-only
  → Re-index only those files
  → Update index in place
```

---

## 5. Query Processing

```
User query: "func.*parse" (regex)

1. Parse query → detect regex
2. Extract trigrams from regex:
   "func" → ["fun", "unc"]
   "parse" → ["par", "ars", "rse"]
3. Pre-filter using trigram index:
   files containing "fun" ∩ "unc" ∩ "par" ∩ "ars" ∩ "rse"
   → 50 candidate files (from 10 million)
4. Run actual regex on 50 files → 8 matches
5. Return results with context (surrounding lines)
```

### Ranking
```
Score = relevance_score(query, result)

Factors:
  - Exact match > partial match
  - Symbol definition > usage (function definition > function call)
  - Filename match > content match
  - Recent files > old files
  - Popular repos > obscure repos
```

---

## 6. Symbol-Aware Search

```
Beyond text search — understand code structure:

Symbol Table:
  Function: "parseConfig" → [(config.py, L42, definition), (main.py, L15, call)]
  Class: "JobScheduler" → [(scheduler.py, L8, definition)]
  Variable: "MAX_RETRIES" → [(constants.py, L3, definition), (worker.py, L22, usage)]

Symbol queries:
  "sym:parseConfig" → find definition of parseConfig
  "sym:parseConfig type:function" → only function definitions
  "ref:parseConfig" → find all usages (calls, imports)

Built by language-specific parsers:
  Python: use ast module
  JavaScript: use tree-sitter
  SQL: custom tokenizer for CREATE/ALTER statements
```

---

## 7. Scaling

```
10 million files, average 200 lines each = 2 billion lines

Sharding strategy:
  Shard by file (each shard handles N files)
  10 shards × 1M files each

Query fan-out:
  Query → all 10 shards in parallel → merge results → return top 100

Index storage:
  Trigram index: ~10GB compressed
  Inverted index: ~5GB compressed
  Symbol table: ~2GB
  Total: ~17GB per shard = 170GB total (fits in memory)
```

### Capacity Estimation
```
Assumptions:
  10M files, average 5KB each = 50TB raw source code
  Trigram index overhead: ~20% of source = 10TB (compressed to ~1TB)
  10 shards, each handling 1M files

Query latency:
  Trigram lookup: <1ms (in-memory)
  Candidate verification (regex on ~100 files): <50ms
  Fan-out + merge across 10 shards: <100ms total

Search QPS:
  1000 queries/sec target
  Each shard handles 1000 QPS (parallelized per query, not per shard)
  Add read replicas if QPS exceeds capacity
```

---

## 8. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Index build time** | Parallelize across workers; incremental re-indexing via git diffs |
| **Memory for index** | Shard index across nodes; keep hot portions in memory, cold on SSD |
| **Regex backtracking** | Timeout regex execution (100ms); reject pathological patterns |
| **Large result sets** | Pagination; show top 100 results; lazy-load more |
| **Binary file noise** | Detect and skip binary files; filter by file extension |
| **Cross-repo search** | Each repo is a shard group; global search fans out across repo shards |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use Elasticsearch?" | "Elasticsearch is great for word-based search but not optimized for substring/regex patterns common in code search. Code search needs trigram indexes for 'find this string inside identifiers.' We can use Elasticsearch as one component but need trigrams for the core search." |
| "How fast is regex on millions of files?" | "We never regex all files. The trigram pre-filter reduces candidates by 99%+. Out of 10M files, trigrams might narrow to 50-100 candidates. Regex on 100 files takes milliseconds." |
| "How do you keep the index fresh?" | "Incremental indexing. We track the last indexed commit per repo. On updates, we only re-index changed files (delta). For a repo with 100K files and 50 changes, we re-index 50 files in seconds, not 100K." |
| "What about searching across branches?" | "We index the default branch by default. Branch-specific search indexes the diff from the base branch. For exact branch search, we overlay the branch's changed files onto the base branch index." |
| "How do you handle very large files?" | "Files above 1MB are flagged and indexed partially (first 10K lines). Generated files (minified JS, vendor dirs) are excluded via .gitignore-style rules." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a code search engine with a **trigram-based index** as the core. Files are crawled, tokenized, and indexed as both trigrams (for substring/regex search) and inverted tokens (for keyword search). Queries extract trigrams to pre-filter candidates, then verify with actual pattern matching — reducing search from 10 million files to ~100 candidates before verification. The index is sharded across nodes and queried in parallel with scatter-gather. Incremental indexing via git diffs keeps the index fresh without full rebuilds. A symbol table adds language-aware search (find definitions, references). Results are ranked by match quality, symbol type, and file relevance."

---

## 11. Key Terms to Drop Naturally

- **Trigram index**, **inverted index**
- **Scatter-gather** (parallel shard queries)
- **Incremental indexing**, **delta updates**
- **Pre-filter + verify** pattern
- **Symbol extraction** (language-aware parsing)
- **Posting list** (list of documents per term)
- **Tree-sitter** (language-aware parser)
- **Content hash** (fast unchanged detection)
