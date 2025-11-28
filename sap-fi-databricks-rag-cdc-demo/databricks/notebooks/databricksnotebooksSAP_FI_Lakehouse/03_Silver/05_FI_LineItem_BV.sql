-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05_FI_LineItem_BV — Silver FI Line Item View (Business Vault Source)
-- MAGIC
-- MAGIC **Purpose:**  
-- MAGIC Create a unified Silver FI Line Item view (`vw_fi_lineitem_bv`) using the Business Vault table `sap_vault.bv_fi_posting` as the single source of truth.
-- MAGIC
-- MAGIC **What it does:**  
-- MAGIC - Selects FI line-item, GL account, and company code data from `bv_fi_posting`.  
-- MAGIC - Exposes clean business fields: document number, line item, posting date, GL descriptions, local and USD amounts, DR/CR flag.  
-- MAGIC - Provides a simplified, fully enriched FI line-item view for downstream Gold models and RAG pipelines.
-- MAGIC
-- MAGIC **Source:**  
-- MAGIC `lending_catalog.sap_vault.bv_fi_posting`
-- MAGIC
-- MAGIC **Output:**  
-- MAGIC `lending_catalog.sap_silver.vw_fi_lineitem_bv`
-- MAGIC

-- COMMAND ----------

-- Databricks notebook source
-- 05_FI_LineItem_BV
-- Silver layer unified FI Line Item view backed by the Business Vault

USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

-- COMMAND ----------

CREATE OR REPLACE VIEW vw_fi_lineitem_bv AS
SELECT
    -- Grain
    fi_document_hk,
    gl_account_hk,
    company_hk,
    belnr          AS document_number,
    bukrs          AS company_code,
    gjahr          AS fiscal_year,
    buzei          AS line_item,

    -- Dates
    posting_date,
    posting_yyyy_mm,
    posting_year,

    -- Currencies & amounts
    doc_curr,
    company_curr,
    amount_doc_curr,
    amount_usd,
    dr_cr_flag,

    -- GL data
    gl_account,
    gl_description,
    gl_account_group,

    -- Company data
    company_name,
    company_country,

    -- Metadata
    load_dts,
    record_source
FROM lending_catalog.sap_vault.bv_fi_posting;

-- COMMAND ----------
SELECT * FROM vw_fi_lineitem_bv
ORDER BY company_code, document_number, line_item
LIMIT 20;
