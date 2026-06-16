"""Real-time AML screening: Pub/Sub -> Apache Beam -> BigQuery.

Each transaction is screened the moment it arrives against per-transaction rules:
  * LCTR          - cash transaction >= 10,000
  * SANCTIONS     - counterparty on the watchlist
  * HIGH_RISK_GEO - counterparty in a high-risk country

Matching transactions produce alert rows written to BigQuery in near-real-time.
(Pattern-based rules that need aggregation over time, such as structuring, are
handled by the batch Airflow pipeline, not here.)

Runs on the local DirectRunner against real Pub/Sub + BigQuery; the same code
runs on Cloud Dataflow with --runner=DataflowRunner.

Usage:
  python aml_stream.py --project PROJECT \
    --subscription projects/PROJECT/subscriptions/aml-transactions-sub \
    --output_table PROJECT:aml.alerts_realtime \
    --temp_location gs://PROJECT-payments-lake/tmp
"""
import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Reference data (kept in sync with the data generator). In production these
# would be loaded as side inputs from the BigQuery watchlist / high-risk tables.
WATCHLIST = {"boris volkov", "aydin sanctioned", "global shell holdings", "red star trading"}
HIGH_RISK_COUNTRIES = {"IR", "KP", "SY", "RU"}


def screen(msg_bytes):
    """Apply per-transaction AML rules; yield an alert row for each rule hit."""
    try:
        t = json.loads(msg_bytes.decode("utf-8"))
    except Exception:
        return

    now = datetime.now(timezone.utc).isoformat()
    base = {
        "alert_time": now,
        "transaction_id": t.get("transaction_id"),
        "customer_id": t.get("customer_id"),
        "amount": t.get("amount"),
    }

    if t.get("channel") == "cash" and (t.get("amount") or 0) >= 10000:
        yield {**base, "rule_code": "LCTR", "severity": "HIGH",
               "detail": f"cash {t.get('amount')}"}

    if (t.get("counterparty_name") or "").lower() in WATCHLIST:
        yield {**base, "rule_code": "SANCTIONS", "severity": "CRITICAL",
               "detail": f"counterparty {t.get('counterparty_name')}"}

    if t.get("counterparty_country") in HIGH_RISK_COUNTRIES:
        yield {**base, "rule_code": "HIGH_RISK_GEO", "severity": "MEDIUM",
               "detail": f"country {t.get('counterparty_country')}"}


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subscription", required=True)
    ap.add_argument("--output_table", required=True)
    args, beam_args = ap.parse_known_args()

    options = PipelineOptions(beam_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True

    schema = ("alert_time:TIMESTAMP,transaction_id:STRING,customer_id:STRING,"
              "rule_code:STRING,severity:STRING,detail:STRING,amount:FLOAT")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.subscription)
            | "Screen" >> beam.FlatMap(screen)
            | "WriteAlerts" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARN)
    run()
