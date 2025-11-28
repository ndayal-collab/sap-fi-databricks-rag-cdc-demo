-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;


-- COMMAND ----------

CREATE OR REPLACE TABLE sat_fi_posting AS
SELECT
  -- Inherit the link key from link_fi_posting
  l.fi_posting_lk,

  -- Measures / attributes at posting grain
  bseg.DMBTR           AS amount_doc_curr,
  bseg.BUZEI           AS buzei,

  -- Metadata
  current_timestamp()  AS load_dts,
  'SAP_BSEG'           AS record_source

FROM lending_catalog.sap_bronze.bseg AS bseg
JOIN link_fi_posting AS l
  ON l.fi_document_hk = sha2(
       concat_ws('|',
         bseg.BELNR,
         bseg.BUKRS,
         CAST(bseg.GJAHR AS STRING)
       ),
       256
     )
 AND l.gl_account_hk = sha2(bseg.HKONT, 256)
 AND l.company_hk    = sha2(bseg.BUKRS, 256)
 AND l.buzei         = bseg.BUZEI;


-- COMMAND ----------

select * 
from sat_fi_posting