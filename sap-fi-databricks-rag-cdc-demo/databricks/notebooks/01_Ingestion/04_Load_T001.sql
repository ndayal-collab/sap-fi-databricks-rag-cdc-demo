-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

SELECT current_catalog(), current_database();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC t001_data = [
-- MAGIC     Row(BUKRS="1000", BUTXT="Company 1000", WAERS="EUR", LAND1="DE"),
-- MAGIC     Row(BUKRS="2000", BUTXT="Company 2000", WAERS="USD", LAND1="US")
-- MAGIC ]
-- MAGIC
-- MAGIC cols_t001 = ["BUKRS", "BUTXT", "WAERS", "LAND1"]
-- MAGIC
-- MAGIC df_t001 = spark.createDataFrame(t001_data, cols_t001)
-- MAGIC df_t001.show()
-- MAGIC
-- MAGIC df_t001.write.mode("overwrite").format("delta").saveAsTable("t001")
-- MAGIC