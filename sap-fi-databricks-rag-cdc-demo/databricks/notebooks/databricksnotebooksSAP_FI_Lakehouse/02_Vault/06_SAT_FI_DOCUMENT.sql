-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE sat_fi_document AS
SELECT
  -- Link to hub
  sha2(
    concat_ws('|',
      bkpf.BELNR,
      bkpf.BUKRS,
      CAST(bkpf.GJAHR AS STRING)
    ),
    256
  )                      AS fi_document_hk,

  -- Descriptive attributes
  bkpf.BLDAT             AS bldat,      -- document date
  bkpf.WAERS             AS doc_curr,   -- document currency

  -- Metadata
  current_timestamp()    AS load_dts,
  'SAP_BKPF'             AS record_source

FROM lending_catalog.sap_bronze.bkpf AS bkpf;


-- COMMAND ----------

SELECT * FROM sat_fi_document;
