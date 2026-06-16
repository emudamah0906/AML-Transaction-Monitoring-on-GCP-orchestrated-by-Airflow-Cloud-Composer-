"""Publish a stream of live transactions to Pub/Sub for real-time AML screening.

Mixes normal traffic with planted suspicious transactions (large cash, watchlist
counterparties, high-risk countries) so the streaming pipeline has hits to catch.

Usage:
  python publish_transactions.py --project PROJECT --topic aml-transactions --count 200 --rate 40
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

CHANNELS = ["cash", "wire", "e-transfer", "card", "transfer"]
COUNTRIES = ["CA", "US", "GB", "IN", "DE"]
HIGH_RISK = ["IR", "KP", "SY", "RU"]
WATCHLIST = ["Boris Volkov", "Aydin Sanctioned", "Global Shell Holdings", "Red Star Trading"]
NORMAL = ["Acme Corp", "Maple Foods", "Northwind Ltd", "Jane Smith", "Bay Retail"]


def make_txn():
    rec = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "txn_timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(20, 4000), 2),
        "currency": "CAD",
        "channel": random.choice(CHANNELS),
        "direction": random.choice(["credit", "debit"]),
        "counterparty_name": random.choice(NORMAL),
        "counterparty_country": random.choice(COUNTRIES),
        "customer_country": "CA",
    }
    roll = random.random()
    if roll < 0.06:                                   # large cash
        rec["channel"], rec["amount"] = "cash", round(random.uniform(10000, 25000), 2)
    elif roll < 0.12:                                 # sanctions hit
        rec["counterparty_name"] = random.choice(WATCHLIST)
    elif roll < 0.18:                                 # high-risk country
        rec["channel"], rec["counterparty_country"] = "wire", random.choice(HIGH_RISK)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="aml-transactions")
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--rate", type=float, default=40)
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    sleep = 1.0 / args.rate if args.rate > 0 else 0

    for _ in range(args.count):
        publisher.publish(topic_path, json.dumps(make_txn()).encode("utf-8"))
        if sleep:
            time.sleep(sleep)
    print(f"Published {args.count} transactions to {topic_path}")


if __name__ == "__main__":
    main()
