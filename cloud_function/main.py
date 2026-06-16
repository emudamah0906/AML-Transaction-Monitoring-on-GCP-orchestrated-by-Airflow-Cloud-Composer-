"""Cloud Function (gen2): event-driven AML file ingestion.

Triggered when a file is finalized in the AML landing bucket. JSON Lines files
(e.g. an upstream transaction feed) are loaded into BigQuery aml.raw_landing.
"""
import os

import functions_framework
from google.cloud import bigquery


@functions_framework.cloud_event
def load_to_bq(cloud_event):
    data = cloud_event.data
    bucket, name = data["bucket"], data["name"]
    if not name.endswith(".jsonl"):
        print(f"Skipping non-jsonl object: {name}")
        return

    uri = f"gs://{bucket}/{name}"
    dataset = os.environ.get("BQ_DATASET", "aml")
    table = f"{dataset}.raw_landing"

    client = bigquery.Client()
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    client.load_table_from_uri(uri, table, job_config=cfg).result()
    out = client.get_table(table)
    print(f"Loaded {uri} -> {table}. Table now has {out.num_rows} rows.")
