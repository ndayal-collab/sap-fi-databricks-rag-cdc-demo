-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC bseg_data = [
-- MAGIC     # BELNR,    BUKRS, GJAHR, BUZEI, HKONT,    DMBTR
-- MAGIC     Row(BELNR="10000001", BUKRS="1000", GJAHR=2024, BUZEI=1, HKONT="400000", DMBTR= 1000.00),
-- MAGIC     Row(BELNR="10000001", BUKRS="1000", GJAHR=2024, BUZEI=2, HKONT="140000", DMBTR=-1000.00),
-- MAGIC     Row(BELNR="10000002", BUKRS="1000", GJAHR=2024, BUZEI=1, HKONT="400000", DMBTR= 1500.00),
-- MAGIC     Row(BELNR="10000002", BUKRS="1000", GJAHR=2024, BUZEI=2, HKONT="140000", DMBTR=-1500.00),
-- MAGIC     Row(BELNR="20000001", BUKRS="2000", GJAHR=2024, BUZEI=1, HKONT="500000", DMBTR= 2000.00),
-- MAGIC     Row(BELNR="20000001", BUKRS="2000", GJAHR=2024, BUZEI=2, HKONT="240000", DMBTR=-2000.00),
-- MAGIC     Row(BELNR="20000002", BUKRS="2000", GJAHR=2024, BUZEI=1, HKONT="500000", DMBTR= 2500.00),
-- MAGIC     Row(BELNR="20000002", BUKRS="2000", GJAHR=2024, BUZEI=2, HKONT="240000", DMBTR=-2500.00),
-- MAGIC ]
-- MAGIC
-- MAGIC cols_bseg = ["BELNR", "BUKRS", "GJAHR", "BUZEI", "HKONT", "DMBTR"]
-- MAGIC
-- MAGIC df_bseg = spark.createDataFrame(bseg_data, cols_bseg)
-- MAGIC df_bseg.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_bseg.write.mode("overwrite").format("delta").saveAsTable("bseg")
-- MAGIC

-- COMMAND ----------

SELECT * FROM bseg ORDER BY BUKRS, BELNR, BUZEI;
