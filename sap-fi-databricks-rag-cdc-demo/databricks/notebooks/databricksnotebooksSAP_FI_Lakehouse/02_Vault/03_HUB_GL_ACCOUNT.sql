-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE hub_gl_account AS
SELECT
  -- Hash key for GL account
  sha2(
    ska1.SAKNR,
    256
  )                      AS gl_account_hk,

  -- Business key
  ska1.SAKNR             AS saknr,

  -- Metadata
  current_timestamp()    AS load_dts,
  'SAP_SKA1'             AS record_source

FROM lending_catalog.sap_bronze.ska1 AS ska1;


-- COMMAND ----------

SELECT * FROM hub_gl_account;
