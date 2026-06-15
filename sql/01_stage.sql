-- Clean and type raw transactions into a staging table.
CREATE OR REPLACE TABLE `aml.stg_transactions` AS
SELECT
  transaction_id,
  customer_id,
  txn_timestamp,
  DATE(txn_timestamp)            AS txn_date,
  CAST(amount AS FLOAT64)        AS amount,
  currency,
  LOWER(channel)                 AS channel,
  direction,
  counterparty_name,
  counterparty_country,
  customer_country
FROM `aml.raw_transactions`
WHERE amount IS NOT NULL AND amount > 0;
