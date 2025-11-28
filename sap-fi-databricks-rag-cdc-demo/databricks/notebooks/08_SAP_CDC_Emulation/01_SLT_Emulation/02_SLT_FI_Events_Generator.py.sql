-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02_SLT_FI_Events_Generator
-- MAGIC
-- MAGIC This notebook simulates SAP SLT change events for FI line items.
-- MAGIC
-- MAGIC It does **not** connect to a real SAP system. Instead, it:
-- MAGIC
-- MAGIC - Reads existing FI line-item data from our Lakehouse (e.g. Silver or DV view)
-- MAGIC - Wraps each row as an SLT-style event with:
-- MAGIC   - `op` = 'I' (insert) for now
-- MAGIC   - FI keys: BELNR, BUKRS, GJAHR, BUZEI
-- MAGIC   - JSON `payload` containing the full row
-- MAGIC   - `event_ts` as the event arrival time
-- MAGIC - Appends these events into `sap_raw.fi_lineitem_events`, which emulates
-- MAGIC   a Kafka topic of SLT change records.
-- MAGIC
-- MAGIC Later we can extend this notebook to also emit:
-- MAGIC - `op = 'U'` events (updates)
-- MAGIC - `op = 'D'` events (deletes)
-- MAGIC for more advanced CDC patterns.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Configure catalog / schema
-- MAGIC spark.sql("USE CATALOG lending_catalog")
-- MAGIC
-- MAGIC # Source FI line-item table or view
-- MAGIC # Adjust this if your actual table name is different.
-- MAGIC source_table = "lending_catalog.sap_silver.fi_lineitem_from_vault"
-- MAGIC
-- MAGIC # Target SLT events "topic" table
-- MAGIC events_table = "lending_catalog.sap_raw.fi_lineitem_events"
-- MAGIC
-- MAGIC print("Source FI table:", source_table)
-- MAGIC print("Target events table:", events_table)
-- MAGIC
-- MAGIC # Load a sample of FI line items to turn into SLT insert events
-- MAGIC # You can change the limit or add filters as needed.
-- MAGIC df_fi = (spark.table(source_table)
-- MAGIC          .limit(50))  # start small; bump later if needed
-- MAGIC
-- MAGIC print("Sample FI rows to convert into events:", df_fi.count())
-- MAGIC display(df_fi.limit(5))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Build SLT-style insert events
-- MAGIC # Assumes the FI dataset has at least:
-- MAGIC #  - belnr (document number)
-- MAGIC #  - bukrs (company code)
-- MAGIC #  - gjahr (fiscal year)
-- MAGIC #  - buzei (line item)
-- MAGIC # If your columns are named differently, adjust here.
-- MAGIC
-- MAGIC event_df = (df_fi
-- MAGIC     .withColumn("op", F.lit("I"))
-- MAGIC     .withColumn("event_ts", F.current_timestamp())
-- MAGIC     .withColumn("payload", F.to_json(F.struct([c for c in df_fi.columns])))
-- MAGIC     .select(
-- MAGIC         "op",
-- MAGIC         F.col("belnr").cast("string").alias("belnr"),
-- MAGIC         F.col("bukrs").cast("string").alias("bukrs"),
-- MAGIC         F.col("gjahr").cast("int").alias("gjahr"),
-- MAGIC         F.col("buzei").cast("string").alias("buzei"),
-- MAGIC         "payload",
-- MAGIC         "event_ts"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC print("Events to write:", event_df.count())
-- MAGIC display(event_df.limit(5))
-- MAGIC
-- MAGIC # Append into the SLT events table
-- MAGIC (event_df
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .saveAsTable(events_table))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Reuse df_fi from earlier as the base FI dataset.
-- MAGIC # Here we pick a subset to act as "updated" rows.
-- MAGIC df_update_base = df_fi.limit(10)
-- MAGIC
-- MAGIC # Build SLT-style UPDATE events (op = 'U')
-- MAGIC update_events_df = (df_update_base
-- MAGIC     .withColumn("op", F.lit("U"))
-- MAGIC     .withColumn("event_ts", F.current_timestamp())
-- MAGIC     .withColumn("payload", F.to_json(F.struct([c for c in df_update_base.columns])))
-- MAGIC     .select(
-- MAGIC         "op",
-- MAGIC         F.col("belnr").cast("string").alias("belnr"),
-- MAGIC         F.col("bukrs").cast("string").alias("bukrs"),
-- MAGIC         F.col("gjahr").cast("int").alias("gjahr"),
-- MAGIC         F.col("buzei").cast("string").alias("buzei"),
-- MAGIC         "payload",
-- MAGIC         "event_ts"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC print("UPDATE events to write:", update_events_df.count())
-- MAGIC display(update_events_df.limit(5))
-- MAGIC
-- MAGIC # Append UPDATE events into the SLT events table
-- MAGIC (update_events_df
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .saveAsTable(events_table))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Pick another subset to act as "deleted" rows.
-- MAGIC # Here we just take a different slice; you can randomize if you like.
-- MAGIC df_delete_base = df_fi.orderBy(F.rand()).limit(10)
-- MAGIC
-- MAGIC # Build SLT-style DELETE events (op = 'D')
-- MAGIC # For deletes, payload can be NULL; the consumer only needs the keys.
-- MAGIC delete_events_df = (df_delete_base
-- MAGIC     .withColumn("op", F.lit("D"))
-- MAGIC     .withColumn("event_ts", F.current_timestamp())
-- MAGIC     .withColumn("payload", F.lit(None).cast("string"))
-- MAGIC     .select(
-- MAGIC         "op",
-- MAGIC         F.col("belnr").cast("string").alias("belnr"),
-- MAGIC         F.col("bukrs").cast("string").alias("bukrs"),
-- MAGIC         F.col("gjahr").cast("int").alias("gjahr"),
-- MAGIC         F.col("buzei").cast("string").alias("buzei"),
-- MAGIC         "payload",
-- MAGIC         "event_ts"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC print("DELETE events to write:", delete_events_df.count())
-- MAGIC display(delete_events_df.limit(5))
-- MAGIC
-- MAGIC # Append DELETE events into the SLT events table
-- MAGIC (delete_events_df
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .saveAsTable(events_table))
-- MAGIC

-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_raw;

SELECT
  event_id,
  op,
  belnr,
  bukrs,
  gjahr,
  buzei,
  event_ts
FROM fi_lineitem_events
ORDER BY event_id DESC
LIMIT 30;
