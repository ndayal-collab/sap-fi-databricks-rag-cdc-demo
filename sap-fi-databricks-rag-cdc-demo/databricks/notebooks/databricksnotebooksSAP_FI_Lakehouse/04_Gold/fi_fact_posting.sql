-- Databricks notebook source
# 01_FI_Fact_Posting — Atomic FI Posting Fact (Gold)

**Purpose:**  
Create the atomic FI posting fact table `fi_fact_posting` at line-item grain using the Silver view `sap_silver.vw_fi_lineitem_bv`.

**Source:**  
`lending_catalog.sap_silver.vw_fi_lineitem_bv`

**Output:**  
`lending_catalog.sap_gold.fi_fact_posting`


-- COMMAND ----------

-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_gold;

-- 1. Create atomic FI posting fact table
CREATE OR REPLACE TABLE fi_fact_posting AS
SELECT
    -- Hash keys
    fi_document_hk,
    gl_account_hk,
    company_hk,

    -- Natural keys
    document_number,
    company_code,
    fiscal_year,
    line_item,

    -- Dates
    posting_date,
    posting_yyyy_mm,
    posting_year,

    -- Currency & amounts
    doc_curr,
    company_curr,
    amount_doc_curr,
    amount_usd,
    dr_cr_flag,

    -- Descriptive fields (denormalized for convenience)
    gl_account,
    gl_description,
    gl_account_group,
    company_name,
    company_country,

    -- Metadata
    load_dts,
    record_source
FROM lending_catalog.sap_silver.vw_fi_lineitem_bv;

-- 2. Quick sanity checks
SELECT COUNT(*) AS fact_rows FROM fi_fact_posting;

SELECT *
FROM fi_fact_posting
ORDER BY company_code, document_number, line_item
LIMIT 20;
