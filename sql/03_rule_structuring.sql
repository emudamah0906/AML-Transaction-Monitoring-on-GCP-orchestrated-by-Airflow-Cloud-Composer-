-- Structuring (smurfing): multiple sub-threshold cash txns by one customer in a day.
CREATE OR REPLACE TABLE `aml.alerts_structuring` AS
WITH s AS (
  SELECT customer_id, txn_date, COUNT(*) AS n, SUM(amount) AS total
  FROM `aml.stg_transactions`
  WHERE channel = 'cash' AND amount BETWEEN 8000 AND 9999.99
  GROUP BY customer_id, txn_date
  HAVING COUNT(*) >= 3 AND SUM(amount) > 10000
)
SELECT
  GENERATE_UUID() AS alert_id,
  customer_id,
  txn_date,
  'STRUCTURING' AS rule_code,
  'Multiple sub-threshold cash transactions (possible smurfing)' AS rule_desc,
  'HIGH' AS severity,
  FORMAT('%d cash txns totalling %.2f', n, total) AS detail,
  total AS amount
FROM s;
