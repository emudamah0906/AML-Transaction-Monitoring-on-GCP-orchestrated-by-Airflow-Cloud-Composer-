-- Name screening: counterparty matches the watchlist.
CREATE OR REPLACE TABLE `aml.alerts_sanctions` AS
SELECT
  GENERATE_UUID() AS alert_id,
  t.customer_id,
  t.txn_date,
  'SANCTIONS' AS rule_code,
  'Counterparty on sanctions / watchlist' AS rule_desc,
  'CRITICAL' AS severity,
  FORMAT('counterparty %s', t.counterparty_name) AS detail,
  t.amount AS amount
FROM `aml.stg_transactions` t
JOIN `aml.watchlist` w ON LOWER(t.counterparty_name) = LOWER(w.name);
