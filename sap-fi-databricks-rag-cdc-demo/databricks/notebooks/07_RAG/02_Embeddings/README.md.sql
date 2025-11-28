-- Databricks notebook source
# 02 – RAG Embeddings

This module defines how embeddings are stored for the documentation RAG.

## Purpose

We extend the RAG document corpus in `sap_ai.doc_rag_corpus` with an
`embedding` column so that each text chunk can be retrieved by similarity
search. This enables LLMs to answer questions about the SAP → Databricks
architecture, Data Vault lineage, data quality strategy, and SAP integration
using grounded documentation.

## Output Tables

### `lending_catalog.sap_ai.doc_rag_embeddings`

Columns:
- `doc_id`      – same as corpus
- `topic`       – high-level classification
- `subtopic`    – more granular classification
- `chunk_index` – ordering within a document
- `text`        – original text chunk
- `source`      – origin label (notebook or doc)
- `embedding`   – array<double>, vector embedding (null in free env)
- `last_updated` – timestamp for audit

## Notebooks

- `02_RAG_Embeddings_Schema.py`
  - Creates `doc_rag_embeddings`
  - Copies rows from `doc_rag_corpus` with `embedding = null`

- `03_RAG_Embeddings_Populate.py`
  - Contains pseudo-code showing how to populate embeddings using
    Databricks AI functions or an external LLM API in a paid environment.
