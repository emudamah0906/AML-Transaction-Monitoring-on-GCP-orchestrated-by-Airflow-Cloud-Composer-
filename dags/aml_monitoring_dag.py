"""AML transaction monitoring pipeline — Apache Airflow DAG (Cloud Composer pattern).

Flow:
    ingest_raw
        -> stage
            -> [ rule_lctr | rule_structuring | rule_sanctions | rule_high_risk ]   (parallel)
                -> merge_alerts
                    -> customer_risk_rating
                        -> ml_features
                            -> data_quality

The SQL transforms run on BigQuery via BigQueryInsertJobOperator (idempotent
CREATE OR REPLACE). Ingestion and the data-quality gate run as Python tasks.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT = "project-40017bb9-a46b-4689-872"
LOCATION = "US"
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def read_sql(name):
    with open(os.path.join(SQL_DIR, name)) as f:
        return f.read()


def ingest_raw(**_):
    """Idempotently (re)load source files into the raw + reference tables."""
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT)
    loads = [
        ("transactions.jsonl", "raw_transactions"),
        ("watchlist.jsonl", "watchlist"),
        ("high_risk_countries.jsonl", "high_risk_countries"),
    ]
    for fname, table in loads:
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(os.path.join(DATA_DIR, fname), "rb") as fh:
            client.load_table_from_file(fh, f"{PROJECT}.aml.{table}", job_config=cfg).result()
    print("Ingest complete.")


def data_quality(**_):
    """Gate: fail the run if core expectations are not met."""
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT)

    def scalar(sql):
        return list(client.query(sql))[0][0]

    failures = []
    stg = scalar("SELECT COUNT(*) FROM `aml.stg_transactions`")
    if stg == 0:
        failures.append("staging is empty")

    alerts = scalar("SELECT COUNT(*) FROM `aml.alerts`")
    parts = scalar(
        "SELECT (SELECT COUNT(*) FROM `aml.alerts_lctr`)"
        " + (SELECT COUNT(*) FROM `aml.alerts_structuring`)"
        " + (SELECT COUNT(*) FROM `aml.alerts_sanctions`)"
        " + (SELECT COUNT(*) FROM `aml.alerts_high_risk`)"
    )
    if alerts != parts:
        failures.append(f"alerts reconciliation mismatch: {alerts} != {parts}")

    crr = scalar("SELECT COUNT(*) FROM `aml.customer_risk_rating`")
    alerted = scalar("SELECT COUNT(DISTINCT customer_id) FROM `aml.alerts`")
    if crr != alerted:
        failures.append(f"CRR customer count mismatch: {crr} != {alerted}")

    print(f"DQ: staging={stg}, alerts={alerts}, crr_customers={crr}")
    if failures:
        raise ValueError("Data quality checks failed: " + "; ".join(failures))
    print("All data quality checks passed.")


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="aml_monitoring",
    description="AML transaction monitoring on GCP/BigQuery",
    schedule=None,            # triggered manually / on demand
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["aml", "gcp", "bigquery"],
) as dag:

    def bq_task(task_id, sql_file):
        return BigQueryInsertJobOperator(
            task_id=task_id,
            project_id=PROJECT,
            location=LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration={"query": {"query": read_sql(sql_file), "useLegacySql": False}},
        )

    ingest = PythonOperator(task_id="ingest_raw", python_callable=ingest_raw)
    stage = bq_task("stage", "01_stage.sql")
    lctr = bq_task("rule_lctr", "02_rule_lctr.sql")
    structuring = bq_task("rule_structuring", "03_rule_structuring.sql")
    sanctions = bq_task("rule_sanctions", "04_rule_sanctions.sql")
    high_risk = bq_task("rule_high_risk", "05_rule_high_risk.sql")
    merge = bq_task("merge_alerts", "06_merge_alerts.sql")
    crr = bq_task("customer_risk_rating", "07_customer_risk_rating.sql")
    ml = bq_task("ml_features", "08_ml_features.sql")
    dq = PythonOperator(task_id="data_quality", python_callable=data_quality)

    ingest >> stage >> [lctr, structuring, sanctions, high_risk] >> merge >> crr >> ml >> dq
