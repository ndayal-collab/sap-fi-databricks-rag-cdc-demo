-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;


-- COMMAND ----------

CREATE OR REPLACE TABLE sat_company_code AS
SELECT
  -- Link to hub
  sha2(
    t001.BUKRS,
    256
  )                      AS company_hk,

  -- Descriptive attributes
  t001.BUTXT             AS company_name,
  t001.WAERS             AS local_curr,
  t001.LAND1             AS country,

  -- Metadata
  current_timestamp()    AS load_dts,
  'SAP_T001'             AS record_source

FROM lending_catalog.sap_bronze.t001 AS t001;


-- COMMAND ----------

SELECT * FROM sat_company_code;
