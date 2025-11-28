-- Databricks notebook source
# 04 – RAG System Architecture

This module documents how the RAG system is architected on top of the
SAP → Databricks Lakehouse.

## Goals

- Explain how documentation RAG is layered:
  - Corpus → Embeddings → Retrieval → LLM
- Show how it answers questions about:
  - Architecture
  - Data Vault lineage
  - Data quality strategy
  - SAP raw ingestion vs BDC + Delta Sharing
- Provide a clear interview story for RAG without needing a full production stack.

## Notebook

- `01_RAG_System_Architecture.py`
  - Markdown-only explanation of the RAG layers and flows.
