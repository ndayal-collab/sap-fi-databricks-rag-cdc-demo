-- Databricks notebook source
-- STEP 1: Use the correct catalog but no schema yet
USE CATALOG lending_catalog;

-- Check where we are
SELECT current_catalog() AS catalog, current_database() AS current_schema;


-- COMMAND ----------

-- STEP 2: Create isolated SAP schemas
CREATE SCHEMA IF NOT EXISTS sap_bronze;
CREATE SCHEMA IF NOT EXISTS sap_silver;
CREATE SCHEMA IF NOT EXISTS sap_gold;


-- COMMAND ----------

-- STEP 3: Verify schemas exist
SHOW SCHEMAS IN lending_catalog;


-- COMMAND ----------

-- STEP 4: Set default working schema
USE SCHEMA sap_bronze;

SELECT current_catalog() AS catalog, current_database() AS schema;
