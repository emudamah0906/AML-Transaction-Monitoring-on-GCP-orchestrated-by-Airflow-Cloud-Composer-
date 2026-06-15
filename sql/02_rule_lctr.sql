-- FINTRAC Large Cash Transaction: a single cash transaction >= 10,000.
CREATE OR REPLACE TABLE `aml.alerts_lctr` AS
SELECT
  GENERATE_UUID() AS alert_id,
  customer_id,
  txn_date,
  'LCTR' AS rule_code,
  'Single cash transaction >= 10,000' AS rule_desc,
  'HIGH' AS severity,
  FORMAT('txn %s, amount %.2f', transaction_id, amount) AS detail,
  amount AS amount
FROM `aml.stg_transactions`
WHERE channel = 'cash' AND amount >= 10000;
