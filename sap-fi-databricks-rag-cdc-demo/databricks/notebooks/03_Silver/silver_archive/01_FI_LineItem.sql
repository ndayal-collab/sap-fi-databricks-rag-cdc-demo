-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE fi_lineitem AS
SELECT
  bseg.BELNR,               -- document number
  bseg.BUKRS,               -- company code
  bseg.GJAHR,               -- fiscal year
  bseg.BUZEI,               -- line item number
  bseg.HKONT,               -- G/L account
  bseg.DMBTR,               -- amount in document currency

  bkpf.BLDAT,               -- document date
  bkpf.WAERS AS DOC_CURR,   -- document currency

  t001.BUTXT AS COMPANY_NAME,
  t001.WAERS AS CO_CODE_CURR,
  t001.LAND1 AS COUNTRY

FROM lending_catalog.sap_bronze.bseg AS bseg
JOIN lending_catalog.sap_bronze.bkpf AS bkpf
  ON bseg.BELNR = bkpf.BELNR
 AND bseg.BUKRS = bkpf.BUKRS
 AND bseg.GJAHR = bkpf.GJAHR
JOIN lending_catalog.sap_bronze.t001 AS t001
  ON bseg.BUKRS = t001.BUKRS;


-- COMMAND ----------

SELECT * FROM fi_lineitem
ORDER BY BUKRS, BELNR, BUZEI;


-- COMMAND ----------

-- Row count should match bseg
SELECT
  (SELECT COUNT(*) FROM lending_catalog.sap_bronze.bseg) AS bseg_rows,
  (SELECT COUNT(*) FROM fi_lineitem) AS fi_lineitem_rows;
