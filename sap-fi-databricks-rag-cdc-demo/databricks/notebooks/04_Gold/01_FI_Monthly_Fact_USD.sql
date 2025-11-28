-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_gold;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

CREATE OR REPLACE TABLE fi_monthly_fact_usd AS
SELECT
    f.bukrs                          AS company_code,
    f.hkont                          AS gl_account,
    date_trunc('month', f.bldat)     AS posting_month,
    'USD'                            AS currency,
    SUM(f.amount_usd)                AS total_amount_usd,
    COUNT(*)                         AS posting_count
FROM lending_catalog.sap_silver.vw_fi_lineitem_usd f
GROUP BY
    f.bukrs,
    f.hkont,
    date_trunc('month', f.bldat);


-- COMMAND ----------

SELECT *
FROM fi_monthly_fact_usd
ORDER BY company_code, gl_account, posting_month
LIMIT 50;
