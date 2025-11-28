-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06_FI_LineItem_Comparison — Validate Old vs New Silver Views
-- MAGIC
-- MAGIC **Purpose:**  
-- MAGIC Compare the old Silver FI line-item view (`vw_fi_lineitem_usd`) with the new BV-based Silver view (`vw_fi_lineitem_bv`) to confirm both produce identical results.
-- MAGIC
-- MAGIC **What it does:**  
-- MAGIC - Compares row counts between old and new views  
-- MAGIC - Checks document-level amount consistency  
-- MAGIC - Uses anti-joins to detect rows that differ or are missing  
-- MAGIC - Ensures the BV-based Silver view fully replaces the old pipeline
-- MAGIC
-- MAGIC **Old Source:**  
-- MAGIC `lending_catalog.sap_silver.vw_fi_lineitem_usd`
-- MAGIC
-- MAGIC **New Source:**  
-- MAGIC `lending_catalog.sap_silver.vw_fi_lineitem_bv`
-- MAGIC

-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- 1. Row count comparison
SELECT 'OLD_vw_fi_lineitem_usd' AS view_name, COUNT(*) AS row_count
FROM vw_fi_lineitem_usd

UNION ALL

SELECT 'NEW_vw_fi_lineitem_bv' AS view_name, COUNT(*) AS row_count
FROM vw_fi_lineitem_bv;


-- COMMAND ----------

-- 2. Compare amounts by document + line
WITH old AS (
    SELECT
        belnr                 AS document_number,
        bukrs                 AS company_code,
        gjahr                 AS fiscal_year,
        buzei                 AS line_item,
        amount_doc_curr,
        amount_usd
    FROM vw_fi_lineitem_usd
),
new AS (
    SELECT
        document_number,
        company_code,
        fiscal_year,
        line_item,
        amount_doc_curr,
        amount_usd
    FROM vw_fi_lineitem_bv
)

SELECT
    COUNT(*) AS total_rows_joined,
    SUM(
        CASE
            WHEN o.amount_doc_curr = n.amount_doc_curr
             AND o.amount_usd      = n.amount_usd
            THEN 0 ELSE 1
        END
    ) AS mismatched_rows
FROM old o
FULL OUTER JOIN new n
  ON  o.document_number = n.document_number
  AND o.company_code    = n.company_code
  AND o.fiscal_year     = n.fiscal_year
  AND o.line_item       = n.line_item;


-- COMMAND ----------

-- 3. Rows in OLD but not in NEW
WITH old AS (
    SELECT
        belnr AS document_number,
        bukrs AS company_code,
        gjahr AS fiscal_year,
        buzei AS line_item
    FROM vw_fi_lineitem_usd
),
new AS (
    SELECT
        document_number,
        company_code,
        fiscal_year,
        line_item
    FROM vw_fi_lineitem_bv
)

SELECT
    'OLD_NOT_IN_NEW' AS diff_type,
    o.*
FROM old o
LEFT ANTI JOIN new n
  ON  o.document_number = n.document_number
  AND o.company_code    = n.company_code
  AND o.fiscal_year     = n.fiscal_year
  AND o.line_item       = n.line_item;


-- COMMAND ----------

-- 4. Rows in NEW but not in OLD
WITH old AS (
    SELECT
        belnr AS document_number,
        bukrs AS company_code,
        gjahr AS fiscal_year,
        buzei AS line_item
    FROM vw_fi_lineitem_usd
),
new AS (
    SELECT
        document_number,
        company_code,
        fiscal_year,
        line_item
    FROM vw_fi_lineitem_bv
)

SELECT
    'NEW_NOT_IN_OLD' AS diff_type,
    n.*
FROM new n
LEFT ANTI JOIN old o
  ON  o.document_number = n.document_number
  AND o.company_code    = n.company_code
  AND o.fiscal_year     = n.fiscal_year
  AND o.line_item       = n.line_item;


-- COMMAND ----------

-- 5. Optional: show any actual value differences
WITH old AS (
    SELECT
        belnr AS document_number,
        bukrs AS company_code,
        gjahr AS fiscal_year,
        buzei AS line_item,
        amount_doc_curr,
        amount_usd
    FROM vw_fi_lineitem_usd
),
new AS (
    SELECT
        document_number,
        company_code,
        fiscal_year,
        line_item,
        amount_doc_curr,
        amount_usd
    FROM vw_fi_lineitem_bv
)

SELECT
    o.document_number,
    o.company_code,
    o.fiscal_year,
    o.line_item,
    o.amount_doc_curr AS old_amount_doc_curr,
    n.amount_doc_curr AS new_amount_doc_curr,
    o.amount_usd      AS old_amount_usd,
    n.amount_usd      AS new_amount_usd
FROM old o
JOIN new n
  ON  o.document_number = n.document_number
  AND o.company_code    = n.company_code
  AND o.fiscal_year     = n.fiscal_year
  AND o.line_item       = n.line_item
WHERE
    (o.amount_doc_curr <> n.amount_doc_curr)
    OR (o.amount_usd <> n.amount_usd);
