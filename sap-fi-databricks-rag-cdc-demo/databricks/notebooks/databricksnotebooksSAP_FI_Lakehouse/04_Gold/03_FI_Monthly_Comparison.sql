-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_FI_Monthly_Comparison — Compare Old vs New Monthly FI Fact
-- MAGIC
-- MAGIC **Purpose:**  
-- MAGIC Compare `fi_monthly_fact_usd` (old) with `fi_monthly_fact_usd_v2` (new, from `fi_fact_posting`) to ensure they have identical results before replacing the old table.
-- MAGIC
-- MAGIC **Old table:**  
-- MAGIC `lending_catalog.sap_gold.fi_monthly_fact_usd`
-- MAGIC
-- MAGIC **New table:**  
-- MAGIC `lending_catalog.sap_gold.fi_monthly_fact_usd_v2`
-- MAGIC

-- COMMAND ----------

-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- 1. Row count comparison
SELECT 'OLD_fi_monthly_fact_usd' AS source, COUNT(*) AS row_count
FROM fi_monthly_fact_usd

UNION ALL

SELECT 'NEW_fi_monthly_fact_usd_v2' AS source, COUNT(*) AS row_count
FROM fi_monthly_fact_usd_v2;


-- COMMAND ----------

-- 2. Value comparison by (company_code, gl_account, posting_month)
WITH old AS (
    SELECT
        company_code,
        gl_account,
        posting_month,
        total_amount_usd,
        posting_count
    FROM fi_monthly_fact_usd
),
new AS (
    SELECT
        company_code,
        gl_account,
        posting_month,
        total_amount_usd,
        posting_count
    FROM fi_monthly_fact_usd_v2
)

SELECT
    COUNT(*) AS total_rows_joined,
    SUM(
        CASE
            WHEN o.total_amount_usd = n.total_amount_usd
             AND o.posting_count    = n.posting_count
            THEN 0 ELSE 1
        END
    ) AS mismatched_rows
FROM old o
FULL OUTER JOIN new n
  ON  o.company_code  = n.company_code
  AND o.gl_account    = n.gl_account
  AND o.posting_month = n.posting_month;


-- COMMAND ----------

-- 3. Rows in OLD but not in NEW
WITH old AS (
    SELECT company_code, gl_account, posting_month
    FROM fi_monthly_fact_usd
),
new AS (
    SELECT company_code, gl_account, posting_month
    FROM fi_monthly_fact_usd_v2
)

SELECT
    'OLD_NOT_IN_NEW' AS diff_type,
    o.*
FROM old o
LEFT ANTI JOIN new n
  ON  o.company_code  = n.company_code
  AND o.gl_account    = n.gl_account
  AND o.posting_month = n.posting_month;


-- COMMAND ----------

-- 4. Rows in NEW but not in OLD
WITH old AS (
    SELECT company_code, gl_account, posting_month
    FROM fi_monthly_fact_usd
),
new AS (
    SELECT company_code, gl_account, posting_month
    FROM fi_monthly_fact_usd_v2
)

SELECT
    'NEW_NOT_IN_OLD' AS diff_type,
    n.*
FROM new n
LEFT ANTI JOIN old o
  ON  o.company_code  = n.company_code
  AND o.gl_account    = n.gl_account
  AND o.posting_month = n.posting_month;
