# Design a Metadata Management Service

> **Interview Prompt:** "Design a service that catalogs and manages metadata for all data assets in a platform."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What types of metadata? (technical, business, operational?) | Scope of the catalog |
| 2 | How is metadata collected? (manual, crawled, event-driven?) | Ingestion architecture |
| 3 | Do we need lineage tracking? (where data came from / goes to?) | Graph model complexity |
| 4 | How many data assets? (hundreds vs. millions?) | Storage and search design |
| 5 | What's the freshness requirement? (near-real-time or daily?) | Crawl frequency |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Data Sources                           │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐      │
│  │Postgr│  │S3    │  │Kafka │  │Spark │  │Snow- │      │
│  │es    │  │      │  │      │  │      │  │flake │      │
│  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘      │
└─────┼────────┼────────┼────────┼────────┼───────────────┘
      │        │        │        │        │
      ▼        ▼        ▼        ▼        ▼
┌──────────────────────────────────────────────────────────┐
│              Metadata Ingestion Layer                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Crawlers     │  │ Event-Driven │  │ Manual Entry │   │
│  │ (Scheduled)  │  │ (CDC/Hooks)  │  │ (UI/API)     │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                  Metadata Store                           │
│  ┌────────────────┐  ┌────────────────┐                  │
│  │  Graph DB       │  │  Search Index  │                  │
│  │  (Neo4j/Neptune)│  │ (Elasticsearch)│                  │
│  │  - Lineage      │  │ - Full-text    │                  │
│  │  - Relationships│  │ - Tag search   │                  │
│  │  - Impact graph │  │ - Faceted      │                  │
│  └────────────────┘  └────────────────┘                  │
│  ┌────────────────┐                                      │
│  │  Relational DB  │                                      │
│  │  (Postgres)     │                                      │
│  │  - Asset details│                                      │
│  │  - Owners/tags  │                                      │
│  │  - Audit log    │                                      │
│  └────────────────┘                                      │
└──────────────────────────┬───────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       ┌──────────┐  ┌──────────┐  ┌──────────┐
       │ Catalog  │  │ Lineage  │  │ Quality  │
       │ UI       │  │ Viewer   │  │ Dashboard│
       └──────────┘  └──────────┘  └──────────┘
```

---

## 3. Deep-Dive: Metadata Types

### 3.1 Technical Metadata (Auto-Discovered)
```
Table: analytics.daily_revenue
  Database: Snowflake
  Schema: analytics
  Columns:
    - date (DATE, not null)
    - revenue (DECIMAL(18,2))
    - region (VARCHAR(50))
    - currency (VARCHAR(3))
  Partitioned by: date
  Row count: 12,453,000
  Size: 2.3 GB
  Created: 2023-01-15
  Last modified: 2024-02-10
  Last queried: 2 minutes ago
```

### 3.2 Business Metadata (Human-Curated)
```
Table: analytics.daily_revenue
  Owner: Data Engineering Team (@alice)
  Description: "Daily revenue aggregated by region and currency. 
                Source: raw transactions from payments service. 
                Updated daily at 3am UTC via ETL pipeline."
  Tags: #finance #revenue #daily #kpi
  Classification: PII=No, Sensitivity=Medium
  Glossary terms: "Revenue" → "Net revenue after refunds"
  Documentation link: confluence.example.com/revenue-metrics
```

### 3.3 Operational Metadata (System-Collected)
```
Table: analytics.daily_revenue
  Freshness:
    Expected: updated daily by 4am UTC
    Last update: 2024-02-10 03:42 UTC ✅ (on time)
    SLA breaches (30d): 1
  Usage:
    Queries/week: 542
    Unique users/week: 23
    Top query patterns: GROUP BY region, WHERE date > ...
  Quality:
    Null rate: 0.01% (healthy)
    Schema changes (90d): 0
    Row count trend: +2% monthly (expected)
```

---

## 4. Data Lineage (Graph Model)

### 4.1 Lineage DAG

```
Data lineage answers: "Where did this data come from?" and 
                       "What depends on this data?"

S3: raw_orders.csv
       │
       ▼ (Spark ETL job, daily 2am)
Snowflake: staging.orders
       │
       ├──▶ Snowflake: analytics.daily_revenue
       │         │
       │         ├──▶ Dashboard: "Revenue KPIs" (Looker)
       │         │
       │         └──▶ Report: "Monthly Board Deck" (Google Sheets)
       │
       └──▶ Snowflake: ml.training_features
                │
                └──▶ ML Model: "Churn Predictor" (SageMaker)
```

### 4.2 Graph Model (Neo4j)
```cypher
// Nodes
(:Dataset {name: "raw_orders.csv", source: "s3://data/raw/"})
(:Table {name: "staging.orders", db: "snowflake"})
(:Table {name: "analytics.daily_revenue", db: "snowflake"})
(:Dashboard {name: "Revenue KPIs", tool: "looker"})
(:Job {name: "ETL Daily Orders", type: "spark"})

// Relationships
(raw_orders)-[:INPUT_TO]->(etl_job)
(etl_job)-[:PRODUCES]->(staging_orders)
(staging_orders)-[:FEEDS]->(daily_revenue)
(daily_revenue)-[:CONSUMED_BY]->(revenue_dashboard)
```

### 4.3 Key Lineage Queries

```
Impact Analysis: "If raw_orders.csv schema changes, what breaks?"
  → Traverse downstream: staging.orders → daily_revenue 
    → Revenue KPIs dashboard → Board Deck report
    → training_features → Churn Predictor model
  Answer: 5 downstream assets affected ⚠️

Root Cause: "Revenue KPIs dashboard shows wrong numbers"
  → Traverse upstream: Revenue KPIs ← daily_revenue 
    ← staging.orders ← raw_orders.csv
  → Check each node's freshness and quality metrics
  Answer: staging.orders ETL job failed at 2am → stale data
```

---

## 5. Search & Discovery

```
User searches: "revenue"

Results ranked by:
  1. Table name match: analytics.daily_revenue (exact)
  2. Column name match: orders.revenue_amount
  3. Description match: "Contains daily revenue aggregations"
  4. Tag match: #revenue #finance
  5. Popularity boost: queried 500 times/week (+boost)
  6. Freshness boost: updated today (+boost)
  7. Owner proximity: same team as searcher (+boost)

Faceted filtering:
  Type: [Tables] [Dashboards] [ML Models] [Files]
  Database: [Snowflake] [Postgres] [S3]
  Owner: [Data Eng] [Analytics] [ML Team]
  Freshness: [Updated today] [This week] [Stale]
  Tags: [#finance] [#customer] [#pii]
```

---

## 6. Crawlers & Ingestion

```
Scheduled crawlers (per source type):

Snowflake Crawler:
  1. Connect via information_schema
  2. Discover all databases/schemas/tables/views
  3. Extract column names, types, constraints
  4. Query history → usage stats
  5. Compare to last crawl → detect schema changes
  6. Update metadata store

S3 Crawler:
  1. List buckets and prefixes
  2. Detect file formats (Parquet, CSV, JSON)
  3. Sample data → infer schema
  4. Collect file sizes, modification times

Kafka Crawler:
  1. List topics from admin API
  2. Fetch schemas from Schema Registry
  3. Collect topic metrics (message rate, lag)

Crawl schedule:
  Technical metadata: every 6 hours
  Operational metadata: every 15 minutes (usage, freshness)
  Business metadata: manual / PR-based
```

---

## 7. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **Stale metadata** | Event-driven updates via CDC for schema changes; scheduled crawls as fallback |
| **Search relevance** | Boost by popularity, freshness, team proximity. Learn from click-through data |
| **Lineage complexity** | Limit traversal depth (max 10 hops). Lazy loading in UI |
| **Crawler overhead** | Incremental crawls (diff since last run). Rate-limit API calls to sources |
| **Manual effort** | Auto-generate descriptions via LLM. Suggest owners based on query patterns |

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "How do you keep metadata fresh?" | "Three mechanisms: (1) scheduled crawlers (every 6h for schema, 15min for stats), (2) event-driven updates via webhooks/CDC when schemas change, (3) query log ingestion for real-time usage tracking. We alert on staleness SLA breaches." |
| "Why a graph DB for lineage?" | "Lineage is inherently a graph — data flows from sources through transforms to destinations. Graph queries like impact analysis ('what breaks if this table changes?') are natural traversals — O(depth) in a graph vs. expensive recursive JOINs in SQL." |
| "How do you handle lineage across different tools?" | "We define a common lineage event format: {source_asset, destination_asset, job_name, timestamp}. Each tool (Spark, dbt, Airflow) emits events in this format. The lineage service stitches them into a unified DAG." |
| "How do you get people to actually use the catalog?" | "Three drivers: (1) auto-populate as much as possible (no manual effort), (2) integrate into tools people already use (Slack bot, IDE plugin, query editor), (3) governance: require catalog registration before data goes to production." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **metadata catalog** with three ingestion layers: crawlers for auto-discovering technical metadata from data sources, event-driven hooks for real-time schema changes, and a UI/API for human-curated business metadata. The store combines a graph database for lineage tracking, Elasticsearch for search/discovery, and Postgres for detailed records. The key features are **lineage visualization** (upstream/downstream impact analysis) and **search with ranking** (by name, description, tags, popularity, freshness). Operational metadata (freshness, usage, quality) is tracked automatically from query logs and pipeline events."

---

## 10. Key Terms to Drop Naturally

- **Data catalog**, **data discovery**
- **Lineage graph**, **impact analysis**, **root cause analysis**
- **Technical / business / operational metadata**
- **Metadata crawling**, **incremental crawl**
- **Freshness SLA**, **data quality score**
- **Information schema** (source for DB metadata)
