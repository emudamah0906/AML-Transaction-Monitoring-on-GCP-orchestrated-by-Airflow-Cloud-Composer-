-- Training-ready feature table per customer, labelled by whether they were alerted.
CREATE OR REPLACE TABLE `aml.ml_features` AS
SELECT
  t.customer_id,
  COUNT(*)                                    AS txn_count,
  COUNTIF(t.channel = 'cash')                 AS cash_count,
  ROUND(SUM(t.amount), 2)                     AS total_amount,
  ROUND(AVG(t.amount), 2)                     AS avg_amount,
  ROUND(MAX(t.amount), 2)                     AS max_amount,
  COUNTIF(t.counterparty_country IN (SELECT country_code FROM `aml.high_risk_countries`)) AS high_risk_txn_count,
  IFNULL(c.alert_count, 0)                    AS alert_count,
  IF(c.customer_id IS NULL, 0, 1)             AS label_has_alert
FROM `aml.stg_transactions` t
LEFT JOIN `aml.customer_risk_rating` c USING (customer_id)
GROUP BY t.customer_id, c.alert_count, c.customer_id;
