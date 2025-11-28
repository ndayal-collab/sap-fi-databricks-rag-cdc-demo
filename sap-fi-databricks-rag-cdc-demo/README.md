# 📘 SAP FI → Databricks Lakehouse  
### CDC Simulation • Data Vault • RAG Assistant • Streamlit UI

This project demonstrates a full SAP Financial Accounting ingestion and analytics pipeline using:

- SAP-like SLT event simulation  
- ODP/Kafka-style CDC ingestion into Bronze  
- Data Vault modeling  
- Silver / Gold layers  
- Delta Log / Snapshot analysis  
- RAG (Retrieval-Augmented Generation) over architecture & lineage  
- Streamlit UI for interactive CDC + RAG exploration  

It simulates a real enterprise migration from **SAP ECC/S/4 → Databricks → Fabric**.

---

# Repository Structure
databricks/
00_setup/
01_slt_simulator/
02_odp_cdc_consumer/
03_rag/
04_delta_snapshots/
05_data_vault/
06_gold/

streamlit_app/
docs/
README.md

---

# Features

### ✔️ SAP CDC Simulation  
SLT-style INSERT / UPDATE / DELETE events.

### ✔️ Bronze ODP Consumer  
Streaming MERGE to maintain latest-image CDC table.

### ✔️ Delta Internals Demo  
Time travel, version diff, snapshot reconstruction.

### ✔️ RAG Assistant  
Semantic + keyword search over architecture docs.

### ✔️ Streamlit UI  
Interactive exploration of CDC + RAG.

---

# Getting Started

### Requirements
- Databricks workspace  
- Python 3.10  
- Streamlit  
- OpenAI or Azure OpenAI (optional)

---

# License  
MIT

