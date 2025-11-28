-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE hub_fi_document AS
SELECT
  -- Data Vault hash key for the FI document business key
  sha2(
    concat_ws('|',
      bkpf.BELNR,
      bkpf.BUKRS,
      CAST(bkpf.GJAHR AS STRING)
    ),
    256
  )                         AS fi_document_hk,

  -- Business key columns
  bkpf.BELNR                AS belnr,
  bkpf.BUKRS                AS bukrs,
  bkpf.GJAHR                AS gjahr,

  -- Metadata
  current_timestamp()       AS load_dts,
  'SAP_BKPF'                AS record_source

FROM lending_catalog.sap_bronze.bkpf AS bkpf;


-- COMMAND ----------

SELECT * FROM hub_fi_document;
