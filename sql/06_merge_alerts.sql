-- Consolidate all rule outputs into a single alerts table.
CREATE OR REPLACE TABLE `aml.alerts` AS
SELECT * FROM `aml.alerts_lctr`
UNION ALL SELECT * FROM `aml.alerts_structuring`
UNION ALL SELECT * FROM `aml.alerts_sanctions`
UNION ALL SELECT * FROM `aml.alerts_high_risk`;
