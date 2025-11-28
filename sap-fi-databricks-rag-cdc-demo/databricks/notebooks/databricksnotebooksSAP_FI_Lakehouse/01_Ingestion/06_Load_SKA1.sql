-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC ska1_data = [
-- MAGIC     # KTOPL,  SAKNR,   TXT50,                 KTONR
-- MAGIC     Row(KTOPL="INT", SAKNR="400000", TXT50="Revenue",               KTONR="4000"),
-- MAGIC     Row(KTOPL="INT", SAKNR="140000", TXT50="Receivables",           KTONR="1400"),
-- MAGIC     Row(KTOPL="INT", SAKNR="500000", TXT50="Service Revenue",       KTONR="5000"),
-- MAGIC     Row(KTOPL="INT", SAKNR="240000", TXT50="Customer Receivable",   KTONR="2400"),
-- MAGIC ]
-- MAGIC
-- MAGIC cols_ska1 = ["KTOPL", "SAKNR", "TXT50", "KTONR"]
-- MAGIC
-- MAGIC df_ska1 = spark.createDataFrame(ska1_data, cols_ska1)
-- MAGIC df_ska1.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_ska1.write.mode("overwrite").format("delta").saveAsTable("ska1")
-- MAGIC

-- COMMAND ----------

SELECT * FROM ska1;
