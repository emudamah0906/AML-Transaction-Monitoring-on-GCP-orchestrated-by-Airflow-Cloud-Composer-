"""Generate synthetic AML transaction data plus reference tables.

Outputs (newline-delimited JSON):
  transactions.jsonl        - customer transactions (with planted suspicious patterns)
  watchlist.jsonl           - sanctioned / watchlisted counterparties (name screening)
  high_risk_countries.jsonl - high-risk country codes

Planted patterns so the AML rules have something to catch:
  * LCTR        - single cash transaction >= 10,000 (FINTRAC large-cash threshold)
  * Structuring - several cash transactions just under 10,000 by one customer in a day
  * Sanctions   - counterparty name on the watchlist
  * High-risk   - counterparty in a high-risk country

Usage:
  python generate_aml_data.py --customers 300 --days 5 --out_dir .
"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

CHANNELS = ["cash", "wire", "e-transfer", "card", "transfer"]
CURRENCIES = ["CAD", "USD"]
COUNTRIES = ["CA", "US", "GB", "IN", "DE", "AE", "SG"]
HIGH_RISK = ["IR", "KP", "SY", "RU"]
WATCHLIST = ["Boris Volkov", "Aydin Sanctioned", "Global Shell Holdings", "Red Star Trading"]
NORMAL_NAMES = ["Acme Corp", "Maple Foods", "Northwind Ltd", "Jane Smith", "Bay Retail", "Tim Stores"]


def base_txn(cust, ts, **over):
    rec = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": cust,
        "txn_timestamp": ts.isoformat(),
        "amount": round(random.uniform(20, 4000), 2),
        "currency": "CAD",
        "channel": random.choice(CHANNELS),
        "direction": random.choice(["credit", "debit"]),
        "counterparty_name": random.choice(NORMAL_NAMES),
        "counterparty_country": random.choice(COUNTRIES),
        "customer_country": "CA",
    }
    rec.update(over)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=300)
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--out_dir", default=".")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    customers = [f"cust_{i:05d}" for i in range(1, args.customers + 1)]
    txns = []

    # Normal background activity
    for _ in range(args.customers * 6):
        cust = random.choice(customers)
        ts = now - timedelta(days=random.randint(0, args.days - 1), seconds=random.randint(0, 86399))
        txns.append(base_txn(cust, ts))

    # Planted LCTR: large single cash transactions
    for cust in random.sample(customers, 8):
        ts = now - timedelta(days=random.randint(0, args.days - 1))
        txns.append(base_txn(cust, ts, channel="cash", amount=round(random.uniform(10000, 25000), 2)))

    # Planted structuring: 4-5 cash txns just under 10k, same customer, same day
    for cust in random.sample(customers, 6):
        day = now - timedelta(days=random.randint(0, args.days - 1))
        for _ in range(random.randint(4, 5)):
            ts = day - timedelta(seconds=random.randint(0, 86399))
            txns.append(base_txn(cust, ts, channel="cash", amount=round(random.uniform(8000, 9900), 2)))

    # Planted sanctions hits: counterparty on the watchlist
    for cust in random.sample(customers, 7):
        ts = now - timedelta(days=random.randint(0, args.days - 1))
        txns.append(base_txn(cust, ts, counterparty_name=random.choice(WATCHLIST)))

    # Planted high-risk country
    for cust in random.sample(customers, 7):
        ts = now - timedelta(days=random.randint(0, args.days - 1))
        txns.append(base_txn(cust, ts, channel="wire", counterparty_country=random.choice(HIGH_RISK),
                             amount=round(random.uniform(3000, 15000), 2)))

    random.shuffle(txns)

    def dump(name, rows):
        with open(os.path.join(args.out_dir, name), "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        print(f"Wrote {len(rows):>5} -> {name}")

    dump("transactions.jsonl", txns)
    dump("watchlist.jsonl", [{"name": n} for n in WATCHLIST])
    dump("high_risk_countries.jsonl", [{"country_code": c} for c in HIGH_RISK])


if __name__ == "__main__":
    main()
