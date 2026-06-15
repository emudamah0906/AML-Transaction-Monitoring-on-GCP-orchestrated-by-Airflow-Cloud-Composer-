-- Geographic risk: counterparty in a high-risk country.
CREATE OR REPLACE TABLE `aml.alerts_high_risk` AS
SELECT
  GENERATE_UUID() AS alert_id,
  t.customer_id,
  t.txn_date,
  'HIGH_RISK_GEO' AS rule_code,
  'Counterparty in high-risk country' AS rule_desc,
  'MEDIUM' AS severity,
  FORMAT('country %s, amount %.2f', t.counterparty_country, t.amount) AS detail,
  t.amount AS amount
FROM `aml.stg_transactions` t
JOIN `aml.high_risk_countries` h ON t.counterparty_country = h.country_code;
