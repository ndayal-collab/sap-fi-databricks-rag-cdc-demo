-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01_RAG_Hybrid_Search
-- MAGIC
-- MAGIC Hybrid (lexical + vector) search over the documentation corpus.
-- MAGIC
-- MAGIC This notebook provides:
-- MAGIC - Simple lexical search (ILIKE) over text
-- MAGIC - Filtering by topic (architecture, data_vault, data_quality, sap_integration)
-- MAGIC - Pseudo-code for vector similarity search
-- MAGIC - The retrieval pattern used in a RAG pipeline
-- MAGIC
-- MAGIC This works fully in free Databricks (lexical now) and documents
-- MAGIC how vector search would be implemented later (e.g., Databricks Vector Search).
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("question", "Explain Delta snapshots", "Question")
-- MAGIC dbutils.widgets.dropdown(
-- MAGIC     "topic",
-- MAGIC     "architecture",
-- MAGIC     ["architecture", "data_vault", "data_quality", "sap_integration", "all"],
-- MAGIC     "Topic"
-- MAGIC )
-- MAGIC
-- MAGIC question = dbutils.widgets.get("question")
-- MAGIC topic = dbutils.widgets.get("topic")
-- MAGIC
-- MAGIC print("Question:", question)
-- MAGIC print("Topic filter:", topic)
-- MAGIC

-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

SELECT
  doc_id,
  topic,
  subtopic,
  chunk_index,
  text
FROM doc_rag_embeddings
WHERE
  (text ILIKE CONCAT('%', '${question}', '%'))
  AND ('${topic}' = 'all' OR topic = '${topic}')
ORDER BY doc_id, chunk_index
LIMIT 20;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vector Similarity (future)
-- MAGIC
-- MAGIC In a full Databricks workspace with vector search capabilities:
-- MAGIC
-- MAGIC 1. Embed the question → query_embedding
-- MAGIC 2. Run ANN / vector similarity against `embedding` in `doc_rag_embeddings`
-- MAGIC 3. Filter by topic (optional)
-- MAGIC 4. Return top-K chunks for LLM context
-- MAGIC
-- MAGIC Example pseudo-SQL:
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   doc_id,
-- MAGIC   topic,
-- MAGIC   subtopic,
-- MAGIC   chunk_index,
-- MAGIC   text,
-- MAGIC   vector_similarity(embedding, :query_embedding) AS score
-- MAGIC FROM sap_ai.doc_rag_embeddings
-- MAGIC WHERE topic = :topic_or_all
-- MAGIC ORDER BY score DESC
-- MAGIC LIMIT 5;
-- MAGIC