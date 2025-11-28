-- Databricks notebook source
USE CATALOG lending_catalog;

SELECT current_catalog();


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sap_vault;


-- COMMAND ----------

SHOW SCHEMAS IN lending_catalog;


-- COMMAND ----------

USE SCHEMA sap_vault;

SELECT current_catalog(), current_database();
