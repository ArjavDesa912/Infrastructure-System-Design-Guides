# Design a "Resume Upload" Parser

> **Interview Prompt:** "Design a system that extracts structured entities from unstructured text — similar to parsing code or extracting data from resumes."

---

## 1. Clarifying Questions to Ask

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What entities are we extracting? (names, skills, dates, SQL objects?) | Defines the extraction schema |
| 2 | What input formats? (PDF, DOCX, plain text, source code?) | Determines the parsing pipeline |
| 3 | What accuracy is acceptable? (90%? 99%?) | ML model vs. rule-based trade-off |
| 4 | What's the throughput? (10/sec or 10K/sec?) | Batch vs. real-time architecture |
| 5 | Do we need confidence scores per extraction? | Affects output schema and downstream decisions |

---

## 2. High-Level Architecture

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Upload   │───▶│  Format      │───▶│  Entity      │───▶│  Post-       │
│  & Ingest │    │  Normalizer  │    │  Extractor   │    │  Processing  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                                                                │
                                                                ▼
                                                        ┌──────────────┐
                                                        │  Structured  │
                                                        │  Output API  │
                                                        └──────────────┘
```

### Pipeline Stages

1. **Upload & Ingest** — Accept files (PDF, DOCX, code files), store in S3
2. **Format Normalizer** — Convert everything to clean plaintext/structured text
3. **Entity Extractor** — Core NLP/ML engine that identifies and classifies entities
4. **Post-Processing** — Validation, deduplication, confidence scoring, normalization
5. **Structured Output** — Return JSON with extracted entities + metadata

---

## 3. Deep-Dive: Core Design Decisions

### 3.1 Format Normalization

```
PDF  ──┐
DOCX ──┤──▶ Text Extractor ──▶ Clean Text ──▶ Section Detector
HTML ──┤                                          │
TXT  ──┘                                          ▼
                                            Structured Sections
                                            {
                                              "header": "John Doe...",
                                              "experience": [...],
                                              "skills": [...],
                                              "education": [...]
                                            }
```

**Tools by format:**
| Format | Extraction Tool |
|--------|----------------|
| PDF | Apache Tika, PyMuPDF, pdfplumber |
| DOCX | python-docx, Apache Tika |
| HTML | BeautifulSoup, lxml |
| Source Code | Tree-sitter, custom lexer |

### 3.2 Entity Extraction Approaches

**Option A: Rule-Based (Regex + Patterns)**
```python
# Example: Extract email
email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# Example: Extract dates
date_pattern = r'(Jan|Feb|Mar|...)\s+\d{4}\s*[-–]\s*(Present|\w+\s+\d{4})'
```
- ✅ Fast, interpretable, no training data needed
- ❌ Brittle, misses variations, hard to maintain

**Option B: ML-Based (NER — Named Entity Recognition)**
```
Input:  "Worked at Google from 2019-2023 using Python and Kafka"
Output: [
    {"text": "Google", "type": "ORG", "confidence": 0.97},
    {"text": "2019-2023", "type": "DATE_RANGE", "confidence": 0.95},
    {"text": "Python", "type": "SKILL", "confidence": 0.99},
    {"text": "Kafka", "type": "SKILL", "confidence": 0.93}
]
```
- ✅ Handles variations, learns patterns
- ❌ Needs training data, black box

**Option C: LLM-Based Extraction**
```
Prompt: "Extract the following fields from this resume text:
         name, email, phone, skills[], experience[{company, role, dates}]
         Return as JSON."
```
- ✅ Highest accuracy for unstructured text, zero-shot
- ❌ Expensive, slower, non-deterministic

**Recommended: Tiered Approach**
```
Level 1: Regex for structured fields (email, phone, URLs)     → fast, cheap
Level 2: ML NER for semi-structured (skills, companies)       → good balance
Level 3: LLM for complex/ambiguous cases (job responsibility) → highest accuracy
```

### 3.3 Scaling the Parser

| Approach | How |
|----------|-----|
| **Batch processing** | Group uploads, process in batches of 100 |
| **Worker pool** | Horizontal scaling via Kubernetes |
| **GPU workers** | Separate pool for ML model inference |
| **LLM queue** | Rate-limited queue for LLM calls with circuit breaker |
| **Caching** | Cache extracted entities for identical documents (hash-based) |

---

## 4. Handling Edge Cases

| Edge Case | Solution |
|-----------|----------|
| **Scanned PDF (image-based)** | OCR pipeline (Tesseract / AWS Textract) before text extraction |
| **Multi-language documents** | Language detection → route to language-specific model |
| **Tables in PDF** | Table detection model (Camelot / Tabula) for structured data |
| **Low confidence extraction** | Flag for human review if confidence < threshold |
| **Duplicate entities** | Deduplication + normalization (e.g., "JS" → "JavaScript") |

### Capacity Estimation

```
Assumptions:
  100K documents/day uploaded
  Average document: 50KB (after normalization)
  Average extractions per document: 20 entities

Storage:
  Documents: 100K × 50KB = 5GB/day in S3
  Entities: 100K × 20 × 200 bytes = 400MB/day in Postgres
  Yearly: ~1.8TB documents, ~146GB entities

Compute:
  Regex extraction: ~50ms/doc → 1 worker handles 20/sec
  NER model: ~200ms/doc (GPU) → 5/sec per GPU
  LLM extraction: ~2s/doc → 0.5/sec per LLM call

Worker pool:
  100K docs/day = ~1.2 docs/sec sustained
  Peak: 10x = 12 docs/sec
  Need: 1 GPU worker + 4 CPU workers + LLM rate-limited pool
```

---

## 5. Data Model

```sql
CREATE TABLE documents (
    doc_id          UUID PRIMARY KEY,
    tenant_id       UUID,
    original_ref    TEXT,         -- S3 URI to original file
    normalized_ref  TEXT,         -- S3 URI to cleaned text
    format          VARCHAR(20), -- pdf, docx, txt, code
    status          ENUM('UPLOADED','NORMALIZING','EXTRACTING','COMPLETE','FAILED'),
    created_at      TIMESTAMP
);

CREATE TABLE extracted_entities (
    entity_id       UUID PRIMARY KEY,
    doc_id          UUID REFERENCES documents,
    entity_type     VARCHAR(50),  -- NAME, EMAIL, SKILL, ORG, DATE_RANGE
    value           TEXT,
    normalized_value TEXT,         -- cleaned/standardized version
    confidence      FLOAT,
    source_method   ENUM('REGEX','NER','LLM'),
    position_start  INT,          -- character offset in text
    position_end    INT,
    created_at      TIMESTAMP
);
```

---

## 6. Security Considerations

```
Data Protection:
  - All documents encrypted at rest in S3 (SSE-KMS)
  - TLS for data in transit
  - PII detection and redaction during extraction
  - Document access logged per-tenant (who accessed what, when)

Input Validation:
  - File type validation (magic bytes, not just extension)
  - Size limits: reject files > 50MB
  - Malware scanning before processing
  - ZIP bomb detection (recursive extraction limits)

Model Security:
  - Prompt injection prevention for LLM calls
  - Rate limiting per tenant to prevent abuse
  - Sanitization of extracted entities (XSS prevention)
  - Audit trail for all extractions (model version, confidence)
```

---

## 6. Testing the Entity Extraction System

```python
# Test: Tiered extraction routing
def test_tiered_extraction():
    document = "Contact: john@example.com, Skills: Python, Kafka"

    result = extract_entities(document)

    # Email should use regex (fast path)
    assert result['entities'][0]['value'] == 'john@example.com'
    assert result['entities'][0]['source_method'] == 'REGEX'
    assert result['entities'][0]['confidence'] > 0.99

    # Skills should use NER
    assert any(e['value'] == 'Python' and e['source_method'] == 'NER'
               for e in result['entities'])

# Test: Low confidence human review flagging
def test_low_confidence_review():
    document = "Complex ambiguous job description..."

    result = extract_entities(document)

    # Entities with confidence < 0.7 should be flagged
    low_confidence = [e for e in result['entities'] if e['confidence'] < 0.7]
    assert len(low_confidence) > 0
    assert all(e['needs_review'] for e in low_confidence)

# Test: PII redaction
def test_pii_redaction():
    document = "SSN: 123-45-6789, Email: test@example.com"

    result = extract_entities(document, redact_pii=True)

    # SSN should be redacted
    assert 'XXX-XX-XXXX' in result['redacted_text']
    assert '123-45-6789' not in result['redacted_text']
```

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "LLMs are expensive for this" | "That's why I use a tiered approach. Regex handles 40% of extractions (emails, phones, URLs) for near-zero cost. ML NER handles another 40% (skills, orgs). LLMs only handle the remaining 20% of complex cases. We can also cache LLM results by document hash." |
| "How do you handle accuracy?" | "We measure precision and recall per entity type. We use confidence thresholds — high-confidence extractions auto-complete, low-confidence ones go to human review. We also have a feedback loop: corrected extractions become training data for the NER model." |
| "What about PII concerns?" | "All documents are stored encrypted at rest. PII fields are flagged during extraction and can be redacted. Access is controlled per-tenant. We follow data retention policies — documents are auto-deleted after N days." |
| "How do you handle poor OCR quality?" | "We assign a document-level quality score based on OCR confidence. Low-quality documents are flagged for re-scan or manual entry. We also use pre-processing (deskew, contrast adjustment) to improve OCR accuracy before extraction." |
| "How do you train the NER model?" | "Start with a pre-trained model (spaCy, Hugging Face). Fine-tune on domain-specific labeled data. The human review corrections feed back into the training loop — every corrected extraction becomes new training data. We retrain weekly and A/B test new models against production." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **multi-stage entity extraction pipeline**. Documents are uploaded and normalized into clean text with section detection. Entity extraction uses a tiered approach: regex for structured fields, a fine-tuned NER model for entities like skills and companies, and an LLM for complex unstructured text. Each extraction includes a confidence score. Low-confidence results are flagged for human review. The system scales horizontally with worker pools, uses caching to avoid re-processing duplicate documents, and maintains a feedback loop where human corrections improve model accuracy. Security includes encryption at rest, PII redaction, malware scanning, and prompt injection prevention for LLM calls."

---

## 9. Key Terms to Drop Naturally

- **NER (Named Entity Recognition)**
- **OCR** (for scanned documents)
- **Confidence score**, **precision/recall**
- **Zero-shot extraction** (LLM)
- **Human-in-the-loop**, **feedback loop**
- **Tiered extraction** (regex → ML → LLM)
- **Active learning** (use uncertain predictions for labeling)
- **Data retention**, **PII redaction**
