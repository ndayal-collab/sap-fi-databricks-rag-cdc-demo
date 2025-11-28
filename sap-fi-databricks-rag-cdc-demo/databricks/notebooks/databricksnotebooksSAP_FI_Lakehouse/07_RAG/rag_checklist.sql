-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

SELECT COUNT(*) AS corpus_rows
FROM doc_rag_corpus;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

SELECT COUNT(*) AS embeddings_rows
FROM doc_rag_embeddings;


-- COMMAND ----------

SELECT doc_id, topic, subtopic, chunk_index, text
FROM doc_rag_corpus
ORDER BY doc_id, chunk_index
LIMIT 10;


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

SELECT doc_id, topic, subtopic, chunk_index, text
FROM doc_rag_embeddings
WHERE text ILIKE '%Data Vault%'
ORDER BY doc_id, chunk_index
LIMIT 10;
