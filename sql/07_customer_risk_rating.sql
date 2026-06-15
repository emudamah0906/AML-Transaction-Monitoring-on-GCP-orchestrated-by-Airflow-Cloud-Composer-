-- Customer Risk Rating: aggregate alert severity into a score and band.
CREATE OR REPLACE TABLE `aml.customer_risk_rating` AS
WITH scored AS (
  SELECT
    customer_id,
    COUNT(*) AS alert_count,
    STRING_AGG(DISTINCT rule_code ORDER BY rule_code) AS rules_triggered,
    SUM(CASE severity WHEN 'CRITICAL' THEN 40 WHEN 'HIGH' THEN 25
                      WHEN 'MEDIUM' THEN 10 ELSE 5 END) AS risk_score
  FROM `aml.alerts`
  GROUP BY customer_id
)
SELECT
  customer_id, alert_count, rules_triggered, risk_score,
  CASE WHEN risk_score >= 40 THEN 'HIGH'
       WHEN risk_score >= 20 THEN 'MEDIUM' ELSE 'LOW' END AS risk_band
FROM scored;
