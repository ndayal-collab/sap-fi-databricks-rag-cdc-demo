-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC from datetime import date
-- MAGIC
-- MAGIC tcurr_data = [
-- MAGIC     Row(fcurr="USD", tcurr="USD", gdatu=date(2025, 1, 1), kursf=1.00,  record_source="TCURR_DEMO"),
-- MAGIC     Row(fcurr="EUR", tcurr="USD", gdatu=date(2025, 1, 1), kursf=1.08,  record_source="TCURR_DEMO"),
-- MAGIC     Row(fcurr="GBP", tcurr="USD", gdatu=date(2025, 1, 1), kursf=1.25,  record_source="TCURR_DEMO"),
-- MAGIC     Row(fcurr="INR", tcurr="USD", gdatu=date(2025, 1, 1), kursf=0.012, record_source="TCURR_DEMO")
-- MAGIC ]
-- MAGIC
-- MAGIC cols_tcurr = ["fcurr", "tcurr", "gdatu", "kursf", "record_source"]
-- MAGIC
-- MAGIC df_tcurr = spark.createDataFrame(tcurr_data, cols_tcurr)
-- MAGIC df_tcurr.show()
-- MAGIC
-- MAGIC df_tcurr.write.mode("overwrite").format("delta").saveAsTable("tcurr")
-- MAGIC

-- COMMAND ----------

SELECT * FROM sap_bronze.tcurr;

