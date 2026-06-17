# Project Report — AML Transaction Monitoring on GCP

**Platform:** Google Cloud (BigQuery, Pub/Sub, Cloud Storage, Cloud Functions) · **Orchestration:** Apache Airflow / Cloud Composer · **Languages:** Python, SQL

## 1. Purpose
Banks must detect money laundering hidden inside large volumes of transactions and
report suspicious activity to the regulator (FINTRAC). This project implements an
automated AML transaction-monitoring platform that ingests transactions, runs
FINTRAC-style detection rules, scores each customer's risk, and prepares a
training-ready dataset for an AML model — guarded by data-quality checks.

## 2. Architecture
Three ingestion modes feed a layered BigQuery model:

| Mode | Trigger | Tech | Use case |
|------|---------|------|----------|
| Batch | scheduled / manual | Airflow + BigQuery | daily detection, structuring, risk rating |
| Streaming | each Pub/Sub message | Pub/Sub + Dataflow (Beam) | real-time per-transaction screening |
| Event-driven | file lands in GCS | Cloud Function | auto-ingest upstream feeds |

**Batch DAG flow:**
```
ingest_raw -> stage -> [ rule_lctr | rule_structuring | rule_sanctions | rule_high_risk ]
           -> merge_alerts -> customer_risk_rating -> ml_features -> data_quality (gate)
```

## 3. Detection rules
| Rule | Logic | Severity |
|------|-------|----------|
| LCTR | single cash transaction ≥ 10,000 (FINTRAC large-cash) | HIGH |
| Structuring | ≥ 3 sub-threshold cash txns by one customer in a day, sum > 10,000 | HIGH |
| Sanctions / name screening | counterparty matches the watchlist | CRITICAL |
| High-risk geography | counterparty in a high-risk country | MEDIUM |

Alerts roll up into a **Customer Risk Rating** (severity-weighted score → LOW/MEDIUM/HIGH),
and an **ml_features** table provides a labelled, training-ready dataset.

## 4. Results (latest run)
Input: **1,848 transactions**, 300 customers, 5 days.

| Stage | Table | Rows |
|-------|-------|------|
| Raw / reference | raw_transactions / watchlist / high_risk_countries | 1,848 / 4 / 4 |
| Staging | stg_transactions | 1,848 |
| Detection | alerts_lctr / structuring / sanctions / high_risk | 8 / 6 / 7 / 7 |
| Consolidated | alerts | 28 |
| Scoring | customer_risk_rating | 27 |
| ML | ml_features | 298 |
| Streaming (real-time) | alerts_realtime | 29 |
| Event-driven | raw_landing | 408 |

**Customer risk bands:** HIGH 7 · MEDIUM 14 · LOW 6.
**Data-quality gate:** passed — alerts reconcile to the sum of rule tables; risk-rating
customer count matches distinct alerted customers.

## 5. Design decisions
- **Right tool per trigger:** batch for patterns needing a full day (structuring); streaming
  for instant red flags (sanctions); event-driven for unpredictable file arrivals.
- **Idempotency:** every task uses `CREATE OR REPLACE` / truncate-reload, so retries and
  backfills never duplicate data.
- **Quality as a gate:** the final task fails the DAG run if reconciliation breaks, so wrong
  numbers never reach a regulator.
- **Layered model:** raw → staging → detection → scoring → ML keeps each concern isolated and
  changes contained.
- **Least-privilege & lineage:** pipeline runs under a dedicated identity; transformations are
  in version control for traceability.

## 6. Cost
Local Airflow (free) orchestrates serverless BigQuery jobs (pennies, within free tier).
No always-on Cloud Composer environment. Cloud Function scales to zero when idle.

## 7. Repository
`dags/aml_monitoring_dag.py` (DAG) · `sql/` (8 transforms) · `streaming/` (Beam real-time) ·
`cloud_function/` (event-driven) · `data/generate_aml_data.py` (synthetic data) · `RUNBOOK.md` (how to run).
