---
title: "SAP FI CDC + RAG Streamlit Demo"
description: "Lightweight demo for CDC event exploration and document retrieval using Databricks or local fallbacks. No LLM implemented yet."
---

# SAP FI CDC + RAG Streamlit Demo

This Streamlit application demonstrates two core capabilities:

### **1. CDC Explorer**
- Displays SAP FI–style change events (Insert / Update / Delete)
- Loads CDC events from the Databricks table:  
  **`lending_catalog.sap_bronze.fi_lineitem_cdc`**
- Falls back to a small hard-coded in-memory CDC dataset when Databricks is unavailable

### **2. Document Retrieval (RAG)**
- Retrieves documentation fragments from the Databricks table:  
  **`lending_catalog.sap_ai.doc_rag_embeddings`**
- Falls back to a small in-memory corpus if Databricks cannot be reached
- **No LLM or OpenAI generation is implemented yet** — retrieval-only demo

---

## Requirements
- Python **3.10+**
- An existing `.env` file (already included with the project)
- Optional: Databricks SQL Warehouse for online mode

The `.env` file is already present and contains the expected Databricks connection fields:

