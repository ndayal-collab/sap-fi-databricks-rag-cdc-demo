-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE link_fi_posting AS
SELECT
  -- Primary key of the link: posting line grain
  sha2(
    concat_ws('|',
      bseg.BELNR,
      bseg.BUKRS,
      CAST(bseg.GJAHR AS STRING),
      bseg.HKONT,
      CAST(bseg.BUZEI AS STRING)
    ),
    256
  ) AS fi_posting_lk,

  -- Foreign keys to hubs (same formulas as hubs)
  sha2(
    concat_ws('|',
      bseg.BELNR,
      bseg.BUKRS,
      CAST(bseg.GJAHR AS STRING)
    ),
    256
  ) AS fi_document_hk,

  sha2(
    bseg.HKONT,
    256
  ) AS gl_account_hk,

  sha2(
    bseg.BUKRS,
    256
  ) AS company_hk,

  -- Driving key at line level
  bseg.BUZEI              AS buzei,

  -- Metadata
  current_timestamp()     AS load_dts,
  'SAP_BSEG'              AS record_source

FROM lending_catalog.sap_bronze.bseg AS bseg;


-- COMMAND ----------

SELECT * 
FROM link_fi_posting
ORDER BY fi_document_hk, buzei;


-- COMMAND ----------

SELECT 
  l.fi_document_hk,
  hdoc.belnr,
  hdoc.bukrs,
  hdoc.gjahr,
  l.gl_account_hk,
  hgl.saknr,
  l.company_hk,
  hco.bukrs AS company_bukrs,
  l.buzei
FROM link_fi_posting AS l
LEFT JOIN hub_fi_document AS hdoc
  ON l.fi_document_hk = hdoc.fi_document_hk
LEFT JOIN hub_gl_account AS hgl
  ON l.gl_account_hk = hgl.gl_account_hk
LEFT JOIN hub_company_code AS hco
  ON l.company_hk = hco.company_hk
ORDER BY hdoc.belnr, l.buzei;
