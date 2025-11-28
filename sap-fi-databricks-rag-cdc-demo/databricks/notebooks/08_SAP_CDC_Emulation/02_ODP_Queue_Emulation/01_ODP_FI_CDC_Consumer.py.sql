-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01_ODP_FI_CDC_Consumer
-- MAGIC
-- MAGIC This notebook emulates an **ODP-style queue consumer** for SAP FI line items.
-- MAGIC
-- MAGIC - Source (fake Kafka / ODP queue):  
-- MAGIC   `lending_catalog.sap_raw.fi_lineitem_events`
-- MAGIC
-- MAGIC - Target (Bronze CDC table with latest image per key):  
-- MAGIC   `lending_catalog.sap_bronze.fi_lineitem_cdc`
-- MAGIC
-- MAGIC Events in `fi_lineitem_events`:
-- MAGIC - Insert / Update / Delete
-- MAGIC - Keys: `belnr`, `bukrs`, `gjahr`, `buzei`
-- MAGIC - `payload`: JSON containing the FI line-item columns
-- MAGIC - `event_ts`: event timestamp
-- MAGIC
-- MAGIC Pattern:
-- MAGIC 1. Read events as a stream.
-- MAGIC 2. Parse JSON `payload` into real columns.
-- MAGIC 3. For each micro-batch, apply a Delta `MERGE` into `fi_lineitem_cdc`:
-- MAGIC    - `D` → delete
-- MAGIC    - `I` / `U` → upsert (insert or update).
-- MAGIC

-- COMMAND ----------

-- Use correct catalog
USE CATALOG lending_catalog;

-- Make sure schemas exist
CREATE SCHEMA IF NOT EXISTS sap_raw;
CREATE SCHEMA IF NOT EXISTS sap_bronze;

-- Volume for streaming checkpoints (DBFS /tmp is not allowed)
CREATE VOLUME IF NOT EXISTS sap_raw.checkpoints;

-- Bronze CDC target table
USE SCHEMA sap_bronze;

CREATE TABLE IF NOT EXISTS fi_lineitem_cdc (
  belnr       STRING,
  bukrs       STRING,
  gjahr       INT,
  buzei       STRING,

  -- business attributes (align roughly to fi_lineitem_from_vault)
  bldat       DATE,
  doc_curr    STRING,
  dmbtr       DOUBLE,
  hkont       STRING,
  company_name STRING,
  country     STRING,

  -- CDC metadata
  last_op       STRING,
  last_event_ts TIMESTAMP,
  load_ts       TIMESTAMP
)
USING DELTA;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Catalog / schema already set in SQL, but safe to repeat:
-- MAGIC spark.sql("USE CATALOG lending_catalog")
-- MAGIC spark.sql("USE SCHEMA sap_raw")
-- MAGIC
-- MAGIC # 1. Table + checkpoint configuration
-- MAGIC source_events_table = "lending_catalog.sap_raw.fi_lineitem_events"
-- MAGIC target_table        = "lending_catalog.sap_bronze.fi_lineitem_cdc"
-- MAGIC checkpoint_path     = "/Volumes/lending_catalog/sap_raw/checkpoints/fi_lineitem_cdc"
-- MAGIC
-- MAGIC print("Source events table:", source_events_table)
-- MAGIC print("Target CDC table   :", target_table)
-- MAGIC print("Checkpoint path     :", checkpoint_path)
-- MAGIC
-- MAGIC # 2. Schema of the JSON payload
-- MAGIC #    This must match what you put into `payload` in the SLT generator.
-- MAGIC payload_schema = """
-- MAGIC   belnr STRING,
-- MAGIC   bukrs STRING,
-- MAGIC   gjahr INT,
-- MAGIC   buzei STRING,
-- MAGIC   bldat DATE,
-- MAGIC   doc_curr STRING,
-- MAGIC   dmbtr DOUBLE,
-- MAGIC   hkont STRING,
-- MAGIC   company_name STRING,
-- MAGIC   country STRING
-- MAGIC """
-- MAGIC
-- MAGIC # 3. Read the events as a stream
-- MAGIC events_stream_df = (
-- MAGIC     spark.readStream
-- MAGIC          .table(source_events_table)
-- MAGIC )
-- MAGIC
-- MAGIC # 4. Parse JSON payload into columns → this defines parsed_stream_df
-- MAGIC parsed_stream_df = (
-- MAGIC     events_stream_df
-- MAGIC     .withColumn("parsed", F.from_json("payload", payload_schema))
-- MAGIC     .select(
-- MAGIC         "op",
-- MAGIC         "event_ts",
-- MAGIC         F.col("parsed.belnr").alias("belnr"),
-- MAGIC         F.col("parsed.bukrs").alias("bukrs"),
-- MAGIC         F.col("parsed.gjahr").alias("gjahr"),
-- MAGIC         F.col("parsed.buzei").alias("buzei"),
-- MAGIC         F.col("parsed.bldat").alias("bldat"),
-- MAGIC         F.col("parsed.doc_curr").alias("doc_curr"),
-- MAGIC         F.col("parsed.dmbtr").alias("dmbtr"),
-- MAGIC         F.col("parsed.hkont").alias("hkont"),
-- MAGIC         F.col("parsed.company_name").alias("company_name"),
-- MAGIC         F.col("parsed.country").alias("country")
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC parsed_stream_df.printSchema()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp
-- MAGIC
-- MAGIC def apply_cdc_to_bronze(microbatch_df, batch_id: int):
-- MAGIC     """
-- MAGIC     Apply CDC events from this micro-batch to the Bronze fi_lineitem_cdc table.
-- MAGIC     """
-- MAGIC     if microbatch_df.isEmpty():
-- MAGIC         return
-- MAGIC
-- MAGIC     microbatch_df = microbatch_df.withColumn("load_ts", current_timestamp())
-- MAGIC     microbatch_df.createOrReplaceTempView("batch_events")
-- MAGIC
-- MAGIC     merge_sql = f"""
-- MAGIC         MERGE INTO {target_table} AS t
-- MAGIC         USING batch_events AS s
-- MAGIC           ON t.belnr = s.belnr
-- MAGIC          AND t.bukrs = s.bukrs
-- MAGIC          AND t.gjahr = s.gjahr
-- MAGIC          AND t.buzei = s.buzei
-- MAGIC
-- MAGIC         WHEN MATCHED AND s.op = 'D' THEN
-- MAGIC           DELETE
-- MAGIC
-- MAGIC         WHEN MATCHED AND s.op IN ('I','U') THEN
-- MAGIC           UPDATE SET
-- MAGIC             t.bldat         = s.bldat,
-- MAGIC             t.doc_curr      = s.doc_curr,
-- MAGIC             t.dmbtr         = s.dmbtr,
-- MAGIC             t.hkont         = s.hkont,
-- MAGIC             t.company_name  = s.company_name,
-- MAGIC             t.country       = s.country,
-- MAGIC             t.last_op       = s.op,
-- MAGIC             t.last_event_ts = s.event_ts,
-- MAGIC             t.load_ts       = s.load_ts
-- MAGIC
-- MAGIC         WHEN NOT MATCHED AND s.op IN ('I','U') THEN
-- MAGIC           INSERT (
-- MAGIC             belnr,
-- MAGIC             bukrs,
-- MAGIC             gjahr,
-- MAGIC             buzei,
-- MAGIC             bldat,
-- MAGIC             doc_curr,
-- MAGIC             dmbtr,
-- MAGIC             hkont,
-- MAGIC             company_name,
-- MAGIC             country,
-- MAGIC             last_op,
-- MAGIC             last_event_ts,
-- MAGIC             load_ts
-- MAGIC           )
-- MAGIC           VALUES (
-- MAGIC             s.belnr,
-- MAGIC             s.bukrs,
-- MAGIC             s.gjahr,
-- MAGIC             s.buzei,
-- MAGIC             s.bldat,
-- MAGIC             s.doc_curr,
-- MAGIC             s.dmbtr,
-- MAGIC             s.hkont,
-- MAGIC             s.company_name,
-- MAGIC             s.country,
-- MAGIC             s.op,
-- MAGIC             s.event_ts,
-- MAGIC             s.load_ts
-- MAGIC           )
-- MAGIC     """
-- MAGIC
-- MAGIC     spark.sql(merge_sql)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = (
-- MAGIC     parsed_stream_df
-- MAGIC         .writeStream
-- MAGIC         .foreachBatch(apply_cdc_to_bronze)     # use the function we just defined
-- MAGIC         .trigger(availableNow=True)            # allowed in serverless
-- MAGIC         .option("checkpointLocation", checkpoint_path)
-- MAGIC         .start()
-- MAGIC )
-- MAGIC
-- MAGIC query.awaitTermination()
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT
  last_op,
  COUNT(*) AS cnt
FROM lending_catalog.sap_bronze.fi_lineitem_cdc
GROUP BY last_op;


-- COMMAND ----------

SELECT *
FROM lending_catalog.sap_bronze.fi_lineitem_cdc
ORDER BY belnr, buzei
LIMIT 20;
