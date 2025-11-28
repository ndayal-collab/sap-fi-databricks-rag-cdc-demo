-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_RAG_Embeddings_Populate
-- MAGIC
-- MAGIC This notebook is a placeholder for populating the `embedding` column
-- MAGIC in `lending_catalog.sap_ai.doc_rag_embeddings`.
-- MAGIC
-- MAGIC In a production or paid Databricks workspace, this would typically:
-- MAGIC - Call an embedding model (e.g., Databricks AI functions, Azure OpenAI, OpenAI)
-- MAGIC - Store the resulting vectors in the `embedding` column
-- MAGIC - Enable vector-based similarity search for documentation RAG
-- MAGIC
-- MAGIC In this POC on Databricks free edition, we keep the logic as pseudo-code
-- MAGIC for architectural discussion and interview purposes.
-- MAGIC

-- COMMAND ----------

-- PSEUDO-CODE ONLY – This will NOT run in a free workspace without AI functions.

-- USE CATALOG lending_catalog;
-- USE SCHEMA sap_ai;
--
-- UPDATE doc_rag_embeddings
-- SET embedding = ai_generate_embedding(
--       'text-embedding-3-large',
--       text
--     ),
--     last_updated = current_timestamp()
-- WHERE embedding IS NULL;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import pandas_udf, col
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC @pandas_udf("array<double>")
-- MAGIC def embed_text_udf(texts: pd.Series) -> pd.Series:
-- MAGIC     # Placeholder: return a fixed vector for each input
-- MAGIC     return pd.Series([[0.0, 0.0, 0.0] for _ in texts])
-- MAGIC
-- MAGIC df = (
-- MAGIC     spark.table("lending_catalog.sap_ai.doc_rag_embeddings")
-- MAGIC     .where("embedding IS NULL")
-- MAGIC )
-- MAGIC
-- MAGIC df_emb = df.withColumn("embedding", embed_text_udf(col("text")))
-- MAGIC
-- MAGIC (
-- MAGIC     df_emb
-- MAGIC     .write
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("lending_catalog.sap_ai.doc_rag_embeddings")
-- MAGIC )