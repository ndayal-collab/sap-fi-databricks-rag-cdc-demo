-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10_Delta_Log_Snapshot_Internals — Delta Logs, Snapshots, Time Travel
-- MAGIC
-- MAGIC **Purpose:**  
-- MAGIC Demonstrate how Delta tables create snapshots and history using Delta logs.  
-- MAGIC Show multiple versions of a table, how to query by `VERSION AS OF`, and why many JSON log files can slow reads.
-- MAGIC
-- MAGIC **Source:**  
-- MAGIC `lending_catalog.sap_gold.fi_fact_posting` (copied into a demo table)
-- MAGIC
-- MAGIC **Demo Table:**  
-- MAGIC `lending_catalog.sap_gold.delta_demo_fi`
-- MAGIC

-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

-- 1) UPDATE: create a Delta version by modifying some rows
UPDATE delta_demo_fi
SET amount_usd = amount_usd * 1.10
WHERE amount_usd > 0;

-- 2) DELETE: remove ALL negative rows (creates another new Delta version)
DELETE FROM delta_demo_fi
WHERE amount_usd < 0;

-- 3) INSERT: add a modified copy of one row (creates another version)
INSERT INTO delta_demo_fi
SELECT
    fi_document_hk,
    gl_account_hk,
    company_hk,
    document_number,
    company_code,
    fiscal_year,
    line_item,
    date_add(posting_date, 1)         AS posting_date,
    posting_yyyy_mm,
    posting_year,
    doc_curr,
    company_curr,
    amount_doc_curr,
    amount_usd,
    dr_cr_flag,
    gl_account,
    gl_description,
    gl_account_group,
    company_name,
    company_country,
    current_timestamp()                AS load_dts,
    'DELTA_DEMO_INSERT'                AS record_source
FROM delta_demo_fi
LIMIT 1;

-- Check row count after all changes
SELECT COUNT(*) AS rows_after_changes FROM delta_demo_fi;


-- COMMAND ----------

-- 1) UPDATE: increase USD amount by 10% for positive amounts
UPDATE delta_demo_fi
SET amount_usd = amount_usd * 1.10
WHERE amount_usd > 0;

-- 2) DELETE: remove one negative row (if any)
-- Delete exactly 1 negative row using a subquery
DELETE FROM delta_demo_fi
WHERE fi_document_hk IN (
    SELECT fi_document_hk
    FROM delta_demo_fi
    WHERE amount_usd < 0
    LIMIT 1
);


-- 3) INSERT: duplicate one row with a different posting_date to show change
INSERT INTO delta_demo_fi
SELECT
    fi_document_hk,
    gl_account_hk,
    company_hk,
    document_number,
    company_code,
    fiscal_year,
    line_item,
    date_add(posting_date, 1)         AS posting_date,
    posting_yyyy_mm,
    posting_year,
    doc_curr,
    company_curr,
    amount_doc_curr,
    amount_usd,
    dr_cr_flag,
    gl_account,
    gl_description,
    gl_account_group,
    company_name,
    company_country,
    current_timestamp()                AS load_dts,
    'DELTA_DEMO_INSERT'                AS record_source
FROM delta_demo_fi
LIMIT 1;

-- Check current row count after changes
SELECT COUNT(*) AS rows_after_changes FROM delta_demo_fi;


-- COMMAND ----------

-- View Delta history to see all versions / operations
DESCRIBE HISTORY delta_demo_fi;


-- COMMAND ----------

WITH
version0 AS (
  SELECT
    COUNT(*)        AS v0_rows,
    SUM(amount_usd) AS v0_total_amount_usd
  FROM delta_demo_fi VERSION AS OF 0
),
latest AS (
  SELECT
    COUNT(*)        AS latest_rows,
    SUM(amount_usd) AS latest_total_amount_usd
  FROM delta_demo_fi
)
SELECT
  v0.v0_rows,
  v0.v0_total_amount_usd,
  l.latest_rows,
  l.latest_total_amount_usd
FROM version0 v0
CROSS JOIN latest l;
