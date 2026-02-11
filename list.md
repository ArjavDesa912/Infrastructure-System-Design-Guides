Core: Migration & Job Scheduling (High Priority)
Design a Distributed Job Scheduler (Crucial: How to run 50k conversion jobs concurrently?)

Design a Code Migration Service (User uploads a 10GB zip of SQL -> You output Snowflake SQL).

Design a Distributed Task Queue (RabbitMQ/Kafka from scratch).

Design a Rate Limiter (Prevent one client from hogging all parser workers).

Design a File Processing Pipeline (Unzip -> Parse -> Convert -> Repackage).

Design a Dead Letter Queue (Handling SQL scripts that crash the parser).

Design a "Resume Upload" Parser (Extracting entities from unstructured text - similar to code parsing).

Design a Distributed Lock Manager (Ensuring two workers don't convert the same file).

Design a Checkpointing System (If a 5-hour migration fails at 99%, how do we resume?).

Design a Progress Bar for a Backend Process (How to push real-time status to the UI).

Data & State Management
Design a Key-Value Store (Focus on consistency).

Design an In-Memory Cache (Redis architecture).

Design a Log Aggregation System (Collecting error logs from conversion workers).

Design a Distributed File System (How to store the source code secure and accessible).

Design a Database Sharding Scheme (Splitting customer metadata).

Design an Idempotent API (Crucial for retries).

Design a Version Control System (Git-lite: storing versions of converted code).

Design a "Diff" Tool (Showing users what changed between Oracle vs. Snowflake SQL).

Design a Pastebin (Storing code snippets with expiration).

Design a Garbage Collector (Cleaning up old temp files after migration).

Snowflake Specifics & Observability
Design a System with Separation of Compute & Storage.

Design a Code Search Engine (Grep at scale).

Design a Metric Monitoring System (Prometheus/Datadog).

Design a Distributed Tracing System.

Design a Schema Registry.

Design a Multi-Tenant SaaS Platform (Isolation between Customer A and Customer B).

Design a Metadata Management Service.

Design a Zero-Copy Cloning Feature.

Design a Change Data Capture (CDC) System.

Design an API Gateway.