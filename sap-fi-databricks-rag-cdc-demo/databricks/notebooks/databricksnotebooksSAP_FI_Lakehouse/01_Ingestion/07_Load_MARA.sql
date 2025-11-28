-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC mara_data = [
-- MAGIC     Row(MATNR="MAT-1000", MTART="FERT", MATKL="FG01", MEINS="EA", MAKTX="Finished Good 1000"),
-- MAGIC     Row(MATNR="MAT-1001", MTART="FERT", MATKL="FG01", MEINS="EA", MAKTX="Finished Good 1001"),
-- MAGIC     Row(MATNR="MAT-2000", MTART="ROH",  MATKL="RM01", MEINS="KG", MAKTX="Raw Material 2000"),
-- MAGIC     Row(MATNR="MAT-2001", MTART="ROH",  MATKL="RM01", MEINS="KG", MAKTX="Raw Material 2001"),
-- MAGIC ]
-- MAGIC
-- MAGIC cols_mara = ["MATNR", "MTART", "MATKL", "MEINS", "MAKTX"]
-- MAGIC
-- MAGIC df_mara = spark.createDataFrame(mara_data, cols_mara)
-- MAGIC df_mara.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_mara.write.mode("overwrite").format("delta").saveAsTable("mara")
-- MAGIC

-- COMMAND ----------

SELECT * FROM mara;
