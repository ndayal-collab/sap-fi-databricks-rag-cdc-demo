-- Databricks notebook source
-- MAGIC %md
-- MAGIC Joins Hubs, Links and Satellites
-- MAGIC
-- MAGIC hub_fi_document, hub_gl_account, hub_company_code
-- MAGIC
-- MAGIC link_fi_posting
-- MAGIC
-- MAGIC sat_fi_document, sat_fi_posting, sat_gl_account, sat_company_code
-- MAGIC
-- MAGIC Joins exchange rates from Bronze sap_bronze.tcurr (latest rate per currency → USD).
-- MAGIC
-- MAGIC Produces derived, business-friendly fields:
-- MAGIC
-- MAGIC Signed posting amount in document currency and USD
-- MAGIC
-- MAGIC Debit/Credit flag based on amount sign
-- MAGIC
-- MAGIC Posting date and posting_yyyy_mm period
-- MAGIC
-- MAGIC GL description, account group, company name, country, currencies
-- MAGIC
-- MAGIC Creates a single, clean table that Silver, Gold and RAG will read from:
-- MAGIC
-- MAGIC Silver views can just SELECT * FROM bv_fi_posting or subset.
-- MAGIC
-- MAGIC Gold facts/dims and RAG embedding pipeline will use this as their source.

-- COMMAND ----------

-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- 1. Prepare latest FX rates (FCURR -> TCURR='USD')
CREATE OR REPLACE TEMP VIEW tcurr_latest_usd AS
WITH ranked AS (
    SELECT
        fcurr,
        tcurr,
        gdatu,
        kursf,
        ROW_NUMBER() OVER (
            PARTITION BY fcurr, tcurr
            ORDER BY gdatu DESC
        ) AS rn
    FROM lending_catalog.sap_bronze.tcurr
)
SELECT
    fcurr,
    tcurr,
    gdatu,
    kursf
FROM ranked
WHERE tcurr = 'USD'
  AND rn = 1;


-- COMMAND ----------

-- 2. Business Vault: FI Posting (line-item grain)
CREATE OR REPLACE TABLE bv_fi_posting AS
SELECT
    -- Hash keys
    l.fi_posting_lk,
    l.fi_document_hk,
    l.gl_account_hk,
    l.company_hk,

    -- Natural keys / grain
    hdoc.belnr,
    hdoc.bukrs,
    hdoc.gjahr,
    l.buzei,

    -- Dates
    sdoc.bldat                               AS posting_date,
    date_format(sdoc.bldat, 'yyyy-MM')       AS posting_yyyy_mm,
    year(sdoc.bldat)                         AS posting_year,

    -- Currencies
    sdoc.doc_curr                            AS doc_curr,
    sco.local_curr                           AS company_curr,

    -- Measures
    sp.amount_doc_curr                       AS amount_doc_curr,
    COALESCE(sp.amount_doc_curr * fx.kursf,  -- convert to USD
             sp.amount_doc_curr)             AS amount_usd,

    -- Simple DR/CR classification from sign
    CASE
        WHEN sp.amount_doc_curr >= 0 THEN 'DEBIT'
        ELSE 'CREDIT'
    END                                       AS dr_cr_flag,

    -- GL master data
    hgl.saknr                                AS gl_account,
    sgl.gl_description,
    sgl.gl_account_group,

    -- Company master data
    hco.bukrs                                AS company_code,
    sco.company_name,
    sco.country                              AS company_country,

    -- Metadata
    current_timestamp()                      AS load_dts,
    'BV_FI_POSTING_DERIVED'                  AS record_source

FROM link_fi_posting            AS l
JOIN hub_fi_document            AS hdoc
  ON l.fi_document_hk = hdoc.fi_document_hk
JOIN sat_fi_document            AS sdoc
  ON sdoc.fi_document_hk = hdoc.fi_document_hk
JOIN sat_fi_posting             AS sp
  ON sp.fi_posting_lk = l.fi_posting_lk
JOIN hub_gl_account             AS hgl
  ON l.gl_account_hk = hgl.gl_account_hk
LEFT JOIN sat_gl_account        AS sgl
  ON sgl.gl_account_hk = hgl.gl_account_hk
JOIN hub_company_code           AS hco
  ON l.company_hk = hco.company_hk
LEFT JOIN sat_company_code      AS sco
  ON sco.company_hk = hco.company_hk
LEFT JOIN tcurr_latest_usd      AS fx
  ON fx.fcurr = sdoc.doc_curr;  -- doc currency -> USD rate


-- COMMAND ----------

-- 3. Quick row count / sanity check
SELECT
  COUNT(*)                          AS bv_rows,
  (SELECT COUNT(*) FROM sat_fi_posting) AS sat_fi_posting_rows;

-- Expect: bv_rows == sat_fi_posting_rows (1:1 with posting lines)


-- COMMAND ----------

-- 4. Sample preview for inspection
SELECT
  belnr,
  bukrs,
  gjahr,
  buzei,
  posting_date,
  doc_curr,
  amount_doc_curr,
  amount_usd,
  dr_cr_flag,
  gl_account,
  gl_description,
  company_code,
  company_name
FROM bv_fi_posting
ORDER BY bukrs, belnr, buzei;
