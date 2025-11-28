-- Databricks notebook source
# 01 – RAG Document Corpus

This module contains the documentation corpus used for Retrieval-Augmented
Generation (RAG) across the SAP → Databricks Lakehouse project.

## Purpose

We store all architecture, data quality, Data Vault lineage, and Delta Lake
explanations in a structured Delta table so they can later be:

- converted into embeddings  
- retrieved by similarity search  
- used by an LLM to answer architectural questions  
- included in a multi-agent workflow (future)

## Output Tables

### `lending_catalog.sap_ai.doc_rag_corpus`

Columns:
- `doc_id` – logical document identifier  
- `topic` – architecture | data_vault | data_quality | sap_integration  
- `subtopic` – more granular categorization  
- `chunk_index` – ordering for multi-chunk documents  
- `text` – the RAG text chunk  
- `source` – notebook or origin label  
- `last_updated` – audit timestamp  

This table is populated by the notebook:

### `01_RAG_Document_Corpus.py`

## Notes

- The corpus includes architecture explanations, DV lineage, Delta log concepts,
  DQ strategy, and SAP BDC vs raw ingestion comparisons.
- The table is read-only for downstream modules.
- Embeddings are created in `07_RAG/02_Embeddings`.
