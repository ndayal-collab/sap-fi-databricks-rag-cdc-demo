-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver View: FI Line Items in USD
-- MAGIC
-- MAGIC ## What this view represents
-- MAGIC This view produces one row per SAP FI posting line, normalized to a common
-- MAGIC group currency (USD). It is designed as the final Silver-level representation
-- MAGIC of finance transactions before aggregation.
-- MAGIC
-- MAGIC ## Source
-- MAGIC - `sap_silver.fi_lineitem_from_vault`  
-- MAGIC   (canonical FI line items reconstructed from the Data Vault)
-- MAGIC
-- MAGIC ## Logic applied
-- MAGIC - Preserves document-level granularity (one row per BSEG line)
-- MAGIC - Joins to SAP currency rates (`TCURR`) to derive USD amounts
-- MAGIC - Does **not** aggregate or filter transactions
-- MAGIC
-- MAGIC ## Why this view exists
-- MAGIC - Decouples currency logic from Gold aggregations
-- MAGIC - Ensures consistent FX handling across all downstream consumers
-- MAGIC - Allows Gold models and ML logic to operate on a stable, normalized dataset
-- MAGIC
-- MAGIC ## Used by
-- MAGIC - Gold FI Monthly Fact (USD)
-- MAGIC - Potential anomaly detection or forecasting models
-- MAGIC

-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_silver;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

CREATE OR REPLACE VIEW vw_fi_lineitem_usd AS
SELECT
    -- Keys
    f.belnr,
    f.bukrs,
    f.gjahr,
    f.buzei,

    -- Dates & currency
    f.bldat,
    f.doc_curr              AS doc_currency,

    -- Amount in document currency
    f.dmbtr                 AS amount_doc_curr,

    -- Company info
    f.company_name,
    f.co_code_curr,
    f.country,

    -- GL info
    f.hkont,
    f.gl_description,
    f.gl_account_group,

    -- FX info
    t.kursf                 AS fx_rate_to_usd,
    f.dmbtr * t.kursf       AS amount_usd

FROM lending_catalog.sap_silver.fi_lineitem_from_vault f
LEFT JOIN lending_catalog.sap_bronze.tcurr t
  ON t.fcurr = f.doc_curr
 AND t.tcurr = 'USD';   -- 🔹 no date predicate here


-- COMMAND ----------

SELECT *
FROM vw_fi_lineitem_usd
ORDER BY bldat DESC
LIMIT 20;
