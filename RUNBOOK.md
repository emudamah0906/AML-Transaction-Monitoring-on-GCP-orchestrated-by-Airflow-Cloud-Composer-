# RUNBOOK — Run the AML pipeline yourself

This runs the **batch pipeline**: local Airflow orchestrates SQL jobs that execute
in **BigQuery (Google Cloud)**. Copy-paste the blocks in order.

> Note: the local Python virtual-env was invalidated when the folder was renamed,
> so Step 1 rebuilds it. You only do Steps 0–1 and 3 once.

```bash
# ---- variables ----
PROJECT=project-40017bb9-a46b-4689-872
cd ~/Desktop/Desktop/gcp-interview-prep/aml-monitoring-pipeline
```

## Step 0 — Authenticate to Google Cloud (once)
```bash
gcloud auth application-default login      # browser opens -> approve
gcloud config set project $PROJECT
```

## Step 1 — Rebuild the Python environment (once)
```bash
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.10.5" "apache-airflow-providers-google" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
```

## Step 2 — Generate the source data
```bash
python data/generate_aml_data.py --customers 300 --days 5 --out_dir data
```

## Step 3 — Configure Airflow (once)
```bash
export AIRFLOW_HOME=$PWD/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export GOOGLE_CLOUD_PROJECT=$PROJECT

airflow db migrate
airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra "{\"project\": \"$PROJECT\"}"
```

## Step 4 — Run the whole pipeline
```bash
# (make sure the 4 export lines from Step 3 are set in this shell)
source .venv/bin/activate
export AIRFLOW_HOME=$PWD/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export GOOGLE_CLOUD_PROJECT=$PROJECT

airflow dags test aml_monitoring 2026-06-20
```
You should see each task marked **SUCCESS**, ending with `data_quality` printing
"All data quality checks passed."

## Step 5 — See the results in BigQuery (Google Cloud)
```bash
# Alerts by rule
bq query --use_legacy_sql=false \
 'SELECT rule_code, COUNT(*) AS alerts FROM `aml.alerts` GROUP BY rule_code ORDER BY alerts DESC'

# Customer risk bands
bq query --use_legacy_sql=false \
 'SELECT risk_band, COUNT(*) AS customers FROM `aml.customer_risk_rating` GROUP BY risk_band'
```
Or open the **BigQuery Console → `aml` dataset** to browse every table, and
**BigQuery Studio → Personal History** to see the jobs Airflow submitted.

## Step 6 — See it visually (optional)
```bash
airflow standalone      # then open http://localhost:8080 (admin password printed in the output)
```
Open the `aml_monitoring` DAG → **Graph** to see the fan-out/fan-in, **Grid** for the run history.
Stop it with `Ctrl+C` (or `pkill -f "airflow standalone"`).

---

## Optional — the other two ingestion modes
These use Apache Beam / Cloud Functions and need a separate Beam env. See
`streaming/aml_stream.py` (Pub/Sub → BigQuery real-time) and `cloud_function/main.py`
(GCS file arrival → BigQuery).

## Cost note
Local Airflow is free; BigQuery jobs at this scale are pennies (free tier). No Cloud
Composer environment is created. Nothing bills hourly.
```
