-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02_FI_Monthly_Fact_V2 — Monthly FI Fact from Atomic Fact
-- MAGIC
-- MAGIC **Purpose:**  
-- MAGIC Create a new monthly FI fact table `fi_monthly_fact_usd_v2` using the atomic fact `fi_fact_posting` (BV/Silver aligned), without changing the existing `fi_monthly_fact_usd` yet.
-- MAGIC
-- MAGIC **Source:**  
-- MAGIC `lending_catalog.sap_gold.fi_fact_posting`
-- MAGIC
-- MAGIC **Output:**  
-- MAGIC `lending_catalog.sap_gold.fi_monthly_fact_usd_v2`
-- MAGIC

-- COMMAND ----------

-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

-- Create new monthly fact from the atomic fact
CREATE OR REPLACE TABLE fi_monthly_fact_usd_v2 AS
SELECT
    company_code,
    gl_account,
    date_trunc('month', posting_date) AS posting_month,
    'USD'                              AS currency,
    SUM(amount_usd)                    AS total_amount_usd,
    COUNT(*)                           AS posting_count
FROM fi_fact_posting
GROUP BY
    company_code,
    gl_account,
    date_trunc('month', posting_date);

-- Quick sanity check
SELECT COUNT(*) AS rows_v2 FROM fi_monthly_fact_usd_v2;

SELECT *
FROM fi_monthly_fact_usd_v2
ORDER BY company_code, gl_account, posting_month
LIMIT 20;


-- COMMAND ----------

