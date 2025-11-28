-- Databricks notebook source
DESCRIBE TABLE fi_fact_posting;
DESCRIBE TABLE fi_monthly_fact_usd;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

WITH
-- 1. Atomic fact vs Silver row counts
fact_counts AS (
    SELECT COUNT(*) AS fact_rows
    FROM fi_fact_posting
),
silver_counts AS (
    SELECT COUNT(*) AS silver_rows
    FROM lending_catalog.sap_silver.vw_fi_lineitem_bv
),

-- 2. Atomic fact profile
fact_profile AS (
    SELECT
        MIN(posting_date)                      AS fact_min_posting_date,
        MAX(posting_date)                      AS fact_max_posting_date,
        COUNT(DISTINCT company_code)           AS fact_distinct_companies,
        COUNT(DISTINCT gl_account)             AS fact_distinct_gl_accounts,
        SUM(amount_usd)                        AS fact_total_amount_usd
    FROM fi_fact_posting
),

-- 3. Monthly fact row count
monthly_counts AS (
    SELECT COUNT(*) AS monthly_rows
    FROM fi_monthly_fact_usd
),

-- 4. Monthly fact profile
monthly_profile AS (
    SELECT
        MIN(posting_month)                     AS monthly_min_month,
        MAX(posting_month)                     AS monthly_max_month,
        COUNT(DISTINCT company_code)           AS monthly_distinct_companies,
        COUNT(DISTINCT gl_account)             AS monthly_distinct_gl_accounts
    FROM fi_monthly_fact_usd
),

-- 5. Monthly total USD
monthly_total AS (
    SELECT
        SUM(total_amount_usd)                  AS monthly_total_amount_usd
    FROM fi_monthly_fact_usd
)

-- Final one-row QA report
SELECT
    fc.fact_rows,
    sc.silver_rows,

    fp.fact_min_posting_date,
    fp.fact_max_posting_date,
    fp.fact_distinct_companies,
    fp.fact_distinct_gl_accounts,
    fp.fact_total_amount_usd,

    mc.monthly_rows,
    mp.monthly_min_month,
    mp.monthly_max_month,
    mp.monthly_distinct_companies,
    mp.monthly_distinct_gl_accounts,
    mt.monthly_total_amount_usd
FROM fact_counts      fc
CROSS JOIN silver_counts  sc
CROSS JOIN fact_profile   fp
CROSS JOIN monthly_counts mc
CROSS JOIN monthly_profile mp
CROSS JOIN monthly_total  mt;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

SELECT
  doc_curr,
  amount_doc_curr,
  amount_usd
FROM fi_fact_posting
ORDER BY posting_date, company_code, gl_account
LIMIT 20;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

SELECT
  SUM(amount_usd)              AS net_amount_usd,
  SUM(ABS(amount_usd))         AS gross_turnover_usd
FROM fi_fact_posting;
