# RAG Corpus Design – Databricks + Local Fallback

This project supports two RAG modes for the Architecture Q&A tab:

1. Online mode — reads the corpus from a Databricks table:
   lending_catalog.sap_ai.doc_rag_corpus

2. Offline mode — uses a small, in-memory fallback corpus

The UI automatically switches between these modes depending on environment configuration.

---

## 1. Databricks-Backed RAG Corpus (Online Mode)

### 1.1 Table Location

The live RAG corpus is stored in:

lending_catalog.sap_ai.doc_rag_corpus

Required columns:
- doc_id (STRING)
- topic (STRING)
- subtopic (STRING)
- text (STRING)

Optional helpful columns:
- chunk_index
- source
- last_updated

### 1.2 Example Inserts

USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

INSERT INTO doc_rag_corpus (doc_id, topic, subtopic, text)
VALUES
(
  'CDC_PATTERN_001',
  'sap_integration',
  'slt_odp_cdc',
  'The SAP FI CDC path emulates SLT/ODP using two main tables...'
),
(
  'CDC_PATTERN_001',
  'sap_integration',
  'slt_odp_cdc',
  'There are three levels to inspect CDC behaviour...'
);

---

## 2. Connecting from Streamlit

The Streamlit app reads Databricks credentials from a .env file located inside streamlit_app:

DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
DATABRICKS_TOKEN=your_token_here

The connector is initialized using databricks-sql-connector:

from databricks import sql as dbsql

conn = dbsql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_TOKEN
)

Then the app queries:

SELECT doc_id, topic, subtopic, text
FROM lending_catalog.sap_ai.doc_rag_corpus
LIMIT 500;

If the query succeeds, the app reports:
"Using RAG corpus from sap_ai.doc_rag_corpus."

---

## 3. Retrieval Logic

Retrieval uses keyword overlap scoring:

score = len(q_tokens & t_tokens)

q_tokens = tokenized user question
t_tokens = tokenized text chunk

The top-k chunks become the answer context.

This is simple, transparent, reproducible, and easy to explain in interviews.

---

## 4. Offline Local Fallback Mode

The fallback corpus is used when:

- SQL connector is not installed
- Env vars are missing or incorrect
- The query fails (table not found, warehouse offline, permissions issue)

In that case the app uses FALLBACK_DOC_RAG_CORPUS and displays the reason in the UI.

Example message:
"Using local fallback corpus. Reason: Databricks error: TABLE_OR_VIEW_NOT_FOUND"

This ensures the app works even offline or on Databricks Free Edition.

---

## 5. Behaviour Summary

1. Load .env
2. Try Databricks connector
3. Query doc_rag_corpus
4. If query returns rows -> use Databricks corpus
5. Otherwise -> use fallback corpus and show reason in UI

This approach is:
- Robust
- Interview-friendly
- Realistic for Databricks architecture
- Ready for future embeddings or vector search
