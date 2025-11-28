-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE hub_company_code AS
SELECT
  -- Hash key for company code
  sha2(
    t001.BUKRS,
    256
  )                      AS company_hk,

  -- Business key
  t001.BUKRS             AS bukrs,

  -- Metadata
  current_timestamp()    AS load_dts,
  'SAP_T001'             AS record_source

FROM lending_catalog.sap_bronze.t001 AS t001;


-- COMMAND ----------

SELECT * FROM hub_company_code;
