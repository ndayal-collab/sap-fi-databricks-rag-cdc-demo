-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

SELECT current_catalog(), current_database();


-- COMMAND ----------

CREATE OR REPLACE TABLE fi_lineitem_enriched AS
SELECT
  fi.BELNR,
  fi.BUKRS,
  fi.GJAHR,
  fi.BUZEI,
  fi.HKONT,
  fi.DMBTR,
  fi.BLDAT,
  fi.DOC_CURR,
  fi.COMPANY_NAME,
  fi.CO_CODE_CURR,
  fi.COUNTRY,

  ska1.TXT50 AS GL_DESCRIPTION,
  ska1.KTONR AS GL_ACCOUNT_GROUP

FROM fi_lineitem AS fi
LEFT JOIN lending_catalog.sap_bronze.ska1 AS ska1
  ON fi.HKONT = ska1.SAKNR;


-- COMMAND ----------

SELECT * 
FROM fi_lineitem_enriched
ORDER BY BUKRS, BELNR, BUZEI;


-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM fi_lineitem) AS fi_lineitem_rows,
  (SELECT COUNT(*) FROM fi_lineitem_enriched) AS enriched_rows;
