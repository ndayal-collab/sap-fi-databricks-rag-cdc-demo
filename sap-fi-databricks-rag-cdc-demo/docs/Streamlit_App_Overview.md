# Streamlit Application Overview
SAP FI CDC + RAG Demo (Databricks + Local Mode)

The Streamlit app provides two major capabilities:

1. CDC Explorer — a visual simulation of SAP SLT/ODP-style change data capture
2. Architecture Q&A — a Retrieval-Augmented Generation (RAG) demo that pulls text chunks from Databricks or from a local fallback corpus

The app automatically adjusts between online and offline modes depending on Databricks connectivity.

---

## 1. Application Structure

The Streamlit app is located in:

streamlit_app/app.py

Supporting files:
- streamlit_app/.env (Databricks credentials)
- streamlit_app/README.md (instructions)
- docs/RAG_Corpus_Design.md (corpus details)
- docs/CDC_Explorer_Design.md (CDC logic)
- docs/Architecture.md (overall architecture)

The app runs locally via:

pip install -r requirements.txt
streamlit run streamlit_app/app.py

---

## 2. Tabs in the UI

The UI has two main tabs:

### Tab 1: CDC Explorer
Shows:
- Insert, Update, Delete events
- Full historical event list
- Current-state view after CDC merge logic
- Timeline of changes

This simulates:
sap_raw.fi_lineitem_events  
sap_bronze.fi_lineitem_cdc

### Tab 2: Architecture Q&A
Provides an interactive question-answer module using RAG.

If Databricks is available:
- loads from lending_catalog.sap_ai.doc_rag_corpus

If Databricks is not available:
- uses the fallback in-memory corpus

---

## 3. Databricks Connection Logic

The app reads credentials from:

streamlit_app/.env

Required variables:
- DATABRICKS_HOST
- DATABRICKS_HTTP_PATH
- DATABRICKS_TOKEN

If all credentials are present and the warehouse responds:
- RAG uses the real Databricks table

If any step fails:
- RAG switches into fallback mode
- A message is shown in the UI explaining the reason

Example message:

"Using local fallback corpus. Reason: TABLE_OR_VIEW_NOT_FOUND"

---

## 4. RAG Retrieval Logic

The app uses simple, explainable keyword-based retrieval:

1. Tokenize user question
2. Tokenize each corpus chunk
3. Score = intersection count of tokens
4. Sort by score
5. Return top 3 chunks

This design is:
- deterministic
- interview-friendly
- requires no embeddings
- easy to replace with real vector search later

---

## 5. CDC Logic (Simulated)

CDC events are stored as a small in-memory list.

Two simulated views:
1. Event history → equivalent to sap_raw.fi_lineitem_events
2. Current state after MERGE → equivalent to sap_bronze.fi_lineitem_cdc

Deletion events remove the record from current state.

This matches real ODP/SLT behavior.

---

## 6. Modes of Operation

### Online Mode (Databricks available)
- RAG pulls from doc_rag_corpus
- CDC still simulated (until a warehouse is provisioned)

### Offline Mode (Databricks unavailable)
- RAG uses fallback corpus
- CDC remains simulated

Both modes ensure the demo always works.

---

## 7. Local Development Instructions

1. Clone the repository
2. Create and activate a virtual environment
3. Install requirements.txt
4. Add a .env file inside streamlit_app/
5. Run the app:

streamlit run streamlit_app/app.py

---

## 8. Extensibility

This app can be evolved by:
- replacing RAG keyword logic with embeddings
- connecting CDC Explorer to real Delta tables
- adding more architecture documents
- adding diagrams, lineage views, or query examples

The current structure is intentionally lightweight for demos, interviews, and offline environments.

---

## 9. Summary

The Streamlit app provides:
- a clear CDC storyline
- a compact RAG example
- offline fallback capability
- real Databricks integration when available


