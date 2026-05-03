# Nedbank Data Engineering Challenge

##  Overview

This project implements a containerized **end-to-end data pipeline** that ingests, transforms, and provisions banking data into a **validated Gold layer**, fully compliant with the challenge scoring system.

The pipeline is built using:

* **PySpark**
* **Delta Lake**
* **Docker (as per interface contract)**

It adheres strictly to:

* Required **input/output paths**
* **Resource constraints** (2GB RAM, 2 vCPU, no network)
* Expected **Gold layer schema and validation queries**

---
## Repository Layout

```
your-submission/
├── Dockerfile                      # Extends nedbank-de-challenge/base:1.0
├── requirements.txt                # Your extra Python dependencies (may be empty)
├── pipeline/
│   ├── __init__.py
│   ├── run_all.py                  # Entry point — do not rename
│   ├── ingest.py                   # Bronze layer — implement this
│   ├── transform.py                # Silver layer — implement this
│   ├── provision.py                # Gold layer — implement this
│   └── stream_ingest.py            # Stage 3 only — implement at Stage 3
├── config/
│   ├── pipeline_config.yaml        # Paths and Spark settings
│   └── dq_rules.yaml               # DQ rules (required from Stage 2)
├── stream/                             # Stage 3 stream data for local testing
│   ├── .gitkeep
│   └── README.md
├── adr/
│   └── stage3_adr.md               # Architecture Decision Record (Stage 3 only)
└── README.md                       # This file
```

##  Architecture (Aligned to Challenge Stages)

### Bronze Layer – Ingestion (`ingest.py`)

* Reads raw files from `/data/input/`
* Writes Delta tables to `/data/output/bronze/`
* Adds:

  * `ingestion_timestamp`
  * `source`

#### Key Decisions

* ❌ No schema inference
* ✔ All fields kept as raw strings (except JSON)

#### Why?

* Prevents schema drift
* Ensures deterministic ingestion under scoring constraints
* Supports reprocessing if downstream fails

---

### Silver Layer – Transformation (`transform.py`)

* Reads Bronze Delta tables
* Performs:

  * Column selection
  * Type casting (dates, numeric fields)
  * Deduplication using `ingestion_timestamp`
  * Flattening nested JSON (transactions)

#### Key Decisions

* Deduplication via window function:

  * Keeps latest record per key
* Explicit type casting:

  * Avoids runtime ambiguity in Spark

#### Trade-offs

* Slightly higher compute cost
* Much higher data quality and consistency

---

### Gold Layer – Provision (`provision.py`)

The Gold layer produces a **fact-style dataset** that satisfies the validation queries and reflects real banking use cases.

#### Core Logic

1. Join transactions → accounts
2. Join accounts → customers
3. Add derived fields
4. Add aggregated metrics
5. Select only required output schema

#### Derived Fields

* `transaction_month`

#### Aggregations (computed from transactions only)

* `transaction_count`
* `total_spent`
* `avg_transaction`

#### Security / Risk Features

* `high_retry_flag` (based on transaction metadata)

#### Why this design?

* Matches expected **fact table structure**
* Ensures **correct aggregation logic** (no duplication from joins)
* Produces **analytics-ready output** for validation and business use

---

##  Docker Implementation (Interface Compliance)

### Base Image

Instead of relying solely on the provided image, a custom build of `Dockerfile.base` was used when needed.

#### Additions:

* Java (required for Spark)
* PySpark
* Delta Lake dependencies

#### Why?

* Ensures environment consistency
* Avoids runtime dependency failures
* Works within **no-network constraint during execution**

#### Trade-offs

* Longer build time
* Larger image size

→ Accepted because scoring prioritizes **reliability over build speed**

---

##  Execution Flow (Scoring-Compatible)

```bash
docker build -t my-submission:test .

docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test
```

✔ Exits with code `0`

✔ Writes to `/data/output/` only

✔ No external dependencies

---

##  Alignment with Validation Queries

The Gold dataset is structured to pass:

* Correct joins across:

  * `account_id`
  * `customer_id`
* Accurate aggregations:

  * Based only on transaction-level data
* Required fields:

  * Exactly matching schema expectations

---

##  Key Design Decisions & Trade-offs

### 1. Aggregations Before Join Duplication

* Built from `transactions` dataset only
* Prevents inflated metrics

---

### 2. Strict Column Selection in Gold

* Only required fields are output
* Ensures validation compatibility

---

### 3. Delta Lake Usage

* Chosen for:

  * Schema enforcement
  * Reliable reads/writes

---

### 4. Deduplication Strategy

* Uses latest `ingestion_timestamp`
* Trade-off: additional computation vs clean data

---

##  Security & Business Context (Nedbank Alignment)

The Gold layer is designed with **banking use cases** in mind:

### Fraud & Risk Indicators

* Retry flag detection
* Transaction behavior aggregation

### Business Use Cases

* Customer spend profiling
* Channel usage analysis
* Monthly reporting

This ensures the dataset is usable for:

* Fraud detection systems
* Risk scoring
* Operational dashboards

---

##  Stage Readiness

### Stage 1 (Current)

✔ Batch pipeline

✔ Validated Gold output

✔ Passes scoring constraints

---

### Stage 2 (Planned)

* Data Quality rules (`dq_rules.yaml`)
* Additional derived features:

  * Transaction velocity
  * Risk scoring indicators

---

### Stage 3 (Planned)

* Streaming ingestion (`stream_ingest.py`)
* Incremental processing
* Architecture Decision Record (`adr/stage3_adr.md`)

---

##  Summary

This submission delivers:

* A **fully compliant pipeline** aligned with challenge requirements
* A **robust data model** (Bronze → Silver → Gold)
* **Correct and efficient aggregation logic**
* Early-stage **security-aware features**

The implementation prioritizes:

* Correctness under constraints
* Reproducibility in Docker
* Readiness for advanced stages

---

