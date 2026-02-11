# Design a Schema Registry

> **Interview Prompt:** "Design a system that manages and enforces data schemas across services."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What serialization format? (Avro, Protobuf, JSON Schema?) | Core schema format support |
| 2 | How many schemas and versions expected? | Storage and lookup performance |
| 3 | Who produces/consumes data? (services, Kafka, ETL?) | Integration points |
| 4 | What compatibility requirements? | Evolution policy |
| 5 | Do we need runtime validation or compile-time only? | Enforcement model |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────┐
│                    Producers                          │
│  Service A ──▶ Serialize ──▶ Kafka Topic              │
│                    │                                   │
│             Schema Registry Client                    │
│             (cache schema locally)                    │
└──────────────┬────────────────────────────────────────┘
               │ Register / lookup schema
               ▼
┌──────────────────────────────────────────────────────┐
│                Schema Registry Service                │
│                                                       │
│  ┌─────────────────────────┐  ┌──────────────────┐   │
│  │  Schema Store            │  │ Compatibility    │   │
│  │  subject → versions →   │  │ Checker          │   │
│  │  schemas                 │  │ (BACKWARD,       │   │
│  │                          │  │  FORWARD, FULL)  │   │
│  └─────────────────────────┘  └──────────────────┘   │
│                                                       │
│  ┌─────────────────────────┐                         │
│  │  ID → Schema Cache       │                         │
│  │  (fast lookup by ID)     │                         │
│  └─────────────────────────┘                         │
└──────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│                    Consumers                          │
│  Kafka Topic ──▶ Deserialize ──▶ Service B            │
│                       │                                │
│                Schema Registry Client                 │
│                (lookup schema by ID)                  │
└──────────────────────────────────────────────────────┘
```

---

## 3. Deep-Dive: Core Design

### 3.1 Schema Storage Model

```
Subject: "orders-value" (typically: topic-name + key/value)
  Version 1: { "type": "record", "fields": [{"name": "id", "type": "int"}] }
  Version 2: { "type": "record", "fields": [{"name": "id", "type": "int"}, 
                                              {"name": "total", "type": "float"}] }
  Version 3: { "type": "record", "fields": [{"name": "id", "type": "int"},
                                              {"name": "total", "type": "float"},
                                              {"name": "currency", "type": "string",
                                               "default": "USD"}] }

Each schema gets a globally unique ID:
  Schema ID 1 → Version 1 of "orders-value"
  Schema ID 2 → Version 2 of "orders-value"
  Schema ID 3 → Version 3 of "orders-value"
```

### 3.2 Wire Format (How Schemas Are Used in Messages)

```
Kafka Message:
  ┌──────────┬────────────┬──────────────────────┐
  │ Magic    │ Schema ID  │ Serialized Data       │
  │ Byte (0) │ (4 bytes)  │ (Avro/Protobuf bytes) │
  └──────────┴────────────┴──────────────────────┘

Producer flow:
  1. Serialize object using schema
  2. Register schema → get schema_id (or use cached ID)
  3. Prepend magic byte + schema_id to serialized data
  4. Send to Kafka

Consumer flow:
  1. Read message, extract schema_id from first 5 bytes
  2. Fetch schema from registry (or use cached)
  3. Deserialize data using the writer's schema
  4. Optionally project into reader's schema (schema evolution)
```

### 3.3 Compatibility Modes

| Mode | Rule | Example |
|------|------|---------|
| **BACKWARD** | New schema can read old data | Adding optional field with default ✅ |
| **FORWARD** | Old schema can read new data | Removing optional field ✅ |
| **FULL** | Both directions compatible | Only adding/removing optional fields with defaults |
| **TRANSITIVE** | Check against ALL previous versions, not just the last | Stricter: BACKWARD_TRANSITIVE, FULL_TRANSITIVE |
| **NONE** | No checks | Free-for-all (not recommended) |

**Compatibility check process:**
```
Register new schema (v3) for subject "orders-value":
  1. Fetch compatibility mode for subject: BACKWARD
  2. Fetch previous version (v2)
  3. Check: can v3 read data written by v2?
     - Added field "currency" with default "USD" → ✅ (old records get default)
  4. Schema registered as version 3, ID = 3
  
Breaking change attempt:
  New schema removes required field "id":
  → Check: can new schema read old data?
  → ❌ Old data has "id" but new schema doesn't expect it
  → Reject with 409 Conflict: "Incompatible schema"
```

### 3.4 Schema Evolution Best Practices

| ✅ Safe Change | Why Safe |
|---------------|----------|
| Add optional field with default | Old data gets default value |
| Remove optional field | New reader ignores missing field |
| Add new enum value (at end) | Forward-compatible |
| Widen numeric type (int → long) | Values still representable |

| ❌ Unsafe Change | Why Unsafe |
|-----------------|-----------|
| Remove required field | Old data has it, new schema doesn't expect it |
| Change field type (int → string) | Breaks deserialization |
| Rename field | Treated as remove + add (breaks compatibility) |
| Reorder fields (in Protobuf) | Field numbers must stay stable |

---

## 4. API Design

```
POST   /subjects/{subject}/versions     — Register new schema
GET    /subjects/{subject}/versions     — List all versions
GET    /subjects/{subject}/versions/{v} — Get specific version
GET    /schemas/ids/{id}                — Get schema by global ID
DELETE /subjects/{subject}/versions/{v} — Soft-delete a version

POST   /compatibility/subjects/{subject}/versions/{v}
       — Check if new schema is compatible with version v

PUT    /config/{subject}               — Set compatibility mode
GET    /config/{subject}               — Get compatibility mode
GET    /subjects                        — List all subjects
```

---

## 5. Caching Strategy

```
Producer-side cache:
  Schema object → Schema ID (avoid re-registration)
  Cache invalidation: never (schemas are immutable once registered)

Consumer-side cache:
  Schema ID → Schema object (avoid repeated lookups)
  Cache invalidation: never (IDs are immutable)

Registry-side cache:
  In-memory LRU cache of all schemas
  Backed by persistent store (Kafka compacted topic or database)
```

**Why Kafka as the backing store?**
- Schema changes are appended as events to a special topic `_schemas`
- Compacted topic retains latest version per key
- Registry instances can rebuild full state on restart by replaying the topic
- Multi-instance registries stay in sync by consuming the same topic
- No need for external database or consensus protocol

---

## 6. Bottlenecks & Solutions

| Bottleneck | Solution |
|------------|----------|
| **High registration rate** | Rare operation; most lookups are reads (cached) |
| **Registry availability** | Clients cache schemas locally; outage only affects new registrations |
| **Schema explosion** | Naming conventions, subject strategies (TopicName, RecordName, TopicRecordName) |
| **Cross-datacenter** | Leader-follower: writes to leader, reads from local follower |
| **Slow compatibility checks** | Pre-compute compatibility on registration; subsequent checks are O(1) |

---

## 7. Subject Naming Strategies

```
TopicNameStrategy (default):
  Topic "orders" → subjects: "orders-key", "orders-value"
  ✅ Simple, one schema per topic
  ❌ Can't share schemas across topics

RecordNameStrategy:
  Schema "com.company.Order" → subject: "com.company.Order"
  ✅ Reuse same schema across multiple topics
  ❌ No per-topic control

TopicRecordNameStrategy:
  Topic "orders" + schema "com.company.Order"
  → subject: "orders-com.company.Order"
  ✅ Most flexible, per-topic-per-record control
```

---

## 8. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use JSON?" | "JSON has no enforced schema — any producer can send any shape. Breaking changes hit production at runtime. A schema registry catches them at registration time, preventing bad data from ever entering Kafka." |
| "Is the registry a SPOF?" | "Clients cache schemas locally and indefinitely (schemas are immutable). If the registry goes down, all existing producers and consumers continue operating. Only new schema registrations fail — a rare, non-critical operation." |
| "How do you handle schema migration for existing data?" | "We don't rewrite existing data. Avro supports reading old data with new schemas through defaults and optional fields. Each message carries its schema ID, so consumers always know which schema to use, even for old messages." |
| "What about Protobuf vs. Avro?" | "Avro stores the schema separately (required for the registry pattern). Protobuf embeds field numbers in the wire format (self-describing). Both work with schema registries. Avro is more common in Kafka ecosystems; Protobuf is more common in gRPC." |

---

## 9. Summary: Your Interview Narrative

> "I'd design a **centralized schema registry** that stores versioned schemas per Kafka subject. Producers register schemas before sending data — each message's first 5 bytes are a magic byte plus a global schema ID. Consumers look up the schema by ID for deserialization. The key feature is **compatibility enforcement**: the registry rejects new schemas that would break existing readers (backward) or writers (forward). Schemas are immutable once registered, so clients cache them forever. The registry itself is backed by a Kafka compacted topic for durability and multi-instance sync — no external database needed."

---

## 10. Key Terms to Drop Naturally

- **Subject**, **schema version**, **schema ID**
- **Avro**, **Protobuf**, **JSON Schema**
- **Backward / forward / full / transitive compatibility**
- **Schema evolution**, **default values**
- **Wire format** (magic byte + schema ID + data)
- **Kafka compacted topic** (as backing store)
- **Writer schema / reader schema** (Avro projection)
