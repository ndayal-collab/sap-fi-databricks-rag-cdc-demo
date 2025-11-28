-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC bkpf_data = [
-- MAGIC     Row(BELNR="10000001", BUKRS="1000", GJAHR=2024, BLDAT="2024-01-15", WAERS="EUR"),
-- MAGIC     Row(BELNR="10000002", BUKRS="1000", GJAHR=2024, BLDAT="2024-01-20", WAERS="EUR"),
-- MAGIC     Row(BELNR="20000001", BUKRS="2000", GJAHR=2024, BLDAT="2024-02-05", WAERS="USD"),
-- MAGIC     Row(BELNR="20000002", BUKRS="2000", GJAHR=2024, BLDAT="2024-02-10", WAERS="USD"),
-- MAGIC ]
-- MAGIC
-- MAGIC df_bkpf = spark.createDataFrame(bkpf_data)
-- MAGIC df_bkpf.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (df_bkpf.write
-- MAGIC     .mode("overwrite")
-- MAGIC     .format("delta")
-- MAGIC     .saveAsTable("bkpf"))
-- MAGIC

-- COMMAND ----------

SELECT * FROM bkpf;
