-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;


-- COMMAND ----------

CREATE OR REPLACE TABLE sat_gl_account AS
SELECT
  -- Link to hub
  sha2(
    ska1.SAKNR,
    256
  )                      AS gl_account_hk,

  -- Descriptive attributes
  ska1.TXT50             AS gl_description,
  ska1.KTONR             AS gl_account_group,

  -- Metadata
  current_timestamp()    AS load_dts,
  'SAP_SKA1'             AS record_source

FROM lending_catalog.sap_bronze.ska1 AS ska1;


-- COMMAND ----------

SELECT * FROM sat_gl_account;
