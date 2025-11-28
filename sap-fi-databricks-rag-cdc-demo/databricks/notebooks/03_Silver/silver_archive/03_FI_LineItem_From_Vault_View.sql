-- Databricks notebook source
CREATE OR REPLACE VIEW fi_lineitem_from_vault AS
SELECT
    -- Keys
    hdoc.belnr,
    hdoc.bukrs,
    hdoc.gjahr,
    l.buzei,

    -- Amount & currency
    sp.amount_doc_curr        AS dmbtr,
    sdoc.bldat,
    sdoc.doc_curr,

    -- Company info
    sco.company_name,
    sco.local_curr            AS co_code_curr,
    sco.country,

    -- GL info
    hgl.saknr                 AS hkont,
    sgl.gl_description,
    sgl.gl_account_group

FROM lending_catalog.sap_vault.link_fi_posting      AS l
JOIN lending_catalog.sap_vault.sat_fi_posting       AS sp
  ON sp.fi_posting_lk = l.fi_posting_lk

JOIN lending_catalog.sap_vault.hub_fi_document      AS hdoc
  ON hdoc.fi_document_hk = l.fi_document_hk
JOIN lending_catalog.sap_vault.sat_fi_document      AS sdoc
  ON sdoc.fi_document_hk = hdoc.fi_document_hk

JOIN lending_catalog.sap_vault.hub_company_code     AS hco
  ON hco.company_hk = l.company_hk
JOIN lending_catalog.sap_vault.sat_company_code     AS sco
  ON sco.company_hk = hco.company_hk

JOIN lending_catalog.sap_vault.hub_gl_account       AS hgl
  ON hgl.gl_account_hk = l.gl_account_hk
JOIN lending_catalog.sap_vault.sat_gl_account       AS sgl
  ON sgl.gl_account_hk = hgl.gl_account_hk;


-- COMMAND ----------

SELECT *
FROM fi_lineitem_from_vault
ORDER BY belnr, buzei;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_silver;

CREATE OR REPLACE TABLE fi_lineitem_from_vault_tbl AS
SELECT *
FROM fi_lineitem_from_vault;


-- COMMAND ----------

SELECT * FROM fi_lineitem_from_vault_tbl;  -- materialized table