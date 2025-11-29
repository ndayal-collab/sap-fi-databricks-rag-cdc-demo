import os
import re
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

# ============================================================================
# 1. Environment + Databricks config
# ============================================================================

APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

try:
    from databricks import sql as dbsql
    HAS_DBSQL = True
except Exception:
    HAS_DBSQL = False

# ============================================================================
# 2. Streamlit page setup
# ============================================================================

st.set_page_config(page_title="SAP FI CDC + RAG Demo", layout="wide")
st.title("SAP FI CDC + RAG Demo")

if DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN:
    st.sidebar.success("Databricks env vars detected (HOST, HTTP_PATH, TOKEN).")
else:
    st.sidebar.warning(
        "Databricks env vars not fully set.\n\n"
        "RAG will use local fallback corpus unless Databricks is available."
    )

# ============================================================================
# 3. RAG corpus loading (Databricks or local demo)
# ============================================================================

FALLBACK_DOC_RAG_CORPUS = [
    {
        "doc_id": "ARCH_OVERVIEW_001",
        "topic": "architecture",
        "subtopic": "sap_to_databricks",
        "company_code": None,
        "company_name": None,
        "text": (
            "In this SAP–Databricks proof of concept, we simulate SAP FI source tables "
            "and land them into the Lakehouse using the Medallion pattern."
        ),
    },
    {
        "doc_id": "DELTA_SNAPSHOTS_001",
        "topic": "architecture",
        "subtopic": "delta_logs",
        "company_code": None,
        "company_name": None,
        "text": (
            "We demonstrate Delta snapshots using DESCRIBE HISTORY and VERSION AS OF."
        ),
    },
]


def load_corpus_from_databricks():
    """Load RAG corpus from lending_catalog.sap_ai.doc_rag_embeddings."""
    if not HAS_DBSQL:
        return None, "databricks-sql-connector not installed"

    if not (DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN):
        return None, "Missing Databricks env vars"

    try:
        conn = dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
        cursor = conn.cursor()

        cursor.execute("USE CATALOG lending_catalog")
        cursor.execute("USE SCHEMA sap_ai")

        cursor.execute("""
            SELECT doc_id, topic, subtopic, company_code, company_name, text
            FROM doc_rag_embeddings
            LIMIT 500
        """)

        rows = []
        for r in cursor.fetchall():
            rows.append({
                "doc_id": r[0],
                "topic": r[1],
                "subtopic": r[2],
                "company_code": r[3],
                "company_name": r[4],
                "text": r[5],
            })

        cursor.close()
        conn.close()

        if not rows:
            return None, "doc_rag_embeddings returned zero rows"

        return rows, None

    except Exception as e:
        return None, f"Databricks error: {e}"


DOC_RAG_CORPUS = FALLBACK_DOC_RAG_CORPUS
RAG_BACKEND = "local"
RAG_ERROR = None

db_rows, err_msg = load_corpus_from_databricks()
if db_rows:
    DOC_RAG_CORPUS = db_rows
    RAG_BACKEND = "databricks"
else:
    RAG_BACKEND = "local"
    RAG_ERROR = err_msg

# ============================================================================
# 4. RAG retrieval
# ============================================================================

def retrieve_chunks(question, topic="all", company_code="", top_k=5):
    question_tokens = set(re.findall(r"\w+", question.lower()))
    topic = (topic or "all").strip()
    company_code = (company_code or "").strip()

    matches = []
    for row in DOC_RAG_CORPUS:
        # Topic filter
        if topic != "all" and row.get("topic") != topic:
            continue

        # Company filter
        if company_code and row.get("company_code") and row["company_code"] != company_code:
            continue

        row_tokens = set(re.findall(r"\w+", row["text"].lower()))
        score = len(question_tokens & row_tokens) if question_tokens else 1

        if score > 0:
            matches.append((score, row))

    matches.sort(key=lambda x: x[0], reverse=True)
    return [row for score, row in matches[:top_k]]

def load_cdc_events_from_databricks(limit=1000):
    """
    Load CDC events from the Bronze CDC table produced by the ODP consumer:

      lending_catalog.sap_bronze.fi_lineitem_cdc

    Columns there:
      belnr, bukrs, gjahr, buzei,
      bldat, doc_curr, dmbtr, hkont, company_name, country,
      last_op, last_event_ts, load_ts

    We map:
      last_op       -> op
      last_event_ts -> event_ts
      doc_curr      -> waers
    """
    if not HAS_DBSQL:
        return None, "databricks-sql-connector not installed"

    if not (DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN):
        return None, "Missing Databricks env vars"

    try:
        conn = dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
        cursor = conn.cursor()

        # Use the Bronze schema where fi_lineitem_cdc lives
        cursor.execute("USE CATALOG lending_catalog")
        cursor.execute("USE SCHEMA sap_bronze")

        cursor.execute(f"""
            SELECT
                belnr,
                bukrs,
                company_name,
                gjahr,
                buzei,
                last_op       AS op,
                hkont,
                dmbtr,
                doc_curr      AS waers,
                last_event_ts AS event_ts
            FROM fi_lineitem_cdc
            ORDER BY last_event_ts
            LIMIT {limit}
        """)

        events = []
        for r in cursor.fetchall():
            events.append({
                "belnr": r[0],
                "bukrs": r[1],
                "company_name": r[2],
                "gjahr": r[3],
                "buzei": r[4],
                "op": r[5],
                "hkont": r[6],
                "dmbtr": float(r[7]),
                "waers": r[8],
                "event_ts": str(r[9]),
            })

        cursor.close()
        conn.close()

        if not events:
            return None, "fi_lineitem_cdc returned zero rows"

        return events, None

    except Exception as e:
        return None, f"Databricks CDC error: {e}"


# ============================================================================
# 5. CDC data + helpers (Databricks with hard-coded fallback)
# ============================================================================

HARDCODED_CDC_EVENTS = [
    # Company 1000, document 10000001 (matches FI_1000_10000001_x)
    {
        "belnr": "10000001", "bukrs": "1000", "company_name": "Company 1000",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "400000",
        "dmbtr": 1000.00, "waers": "EUR", "event_ts": "2024-01-15 09:00:00",
    },
    {
        "belnr": "10000001", "bukrs": "1000", "company_name": "Company 1000",
        "gjahr": 2024, "buzei": 1, "op": "U", "hkont": "400000",
        "dmbtr": 1200.00, "waers": "EUR", "event_ts": "2024-01-15 10:15:00",
    },

    # Company 1000, document 10000002 (matches FI_1000_10000002_x)
    {
        "belnr": "10000002", "bukrs": "1000", "company_name": "Company 1000",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "140000",
        "dmbtr": 1500.00, "waers": "EUR", "event_ts": "2024-01-20 08:45:00",
    },

    # Company 2000, document 20000001 (matches FI_2000_20000001_x)
    {
        "belnr": "20000001", "bukrs": "2000", "company_name": "Company 2000",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "500000",
        "dmbtr": 2000.00, "waers": "USD", "event_ts": "2024-02-05 09:30:00",
    },
    {
        "belnr": "20000001", "bukrs": "2000", "company_name": "Company 2000",
        "gjahr": 2024, "buzei": 1, "op": "U", "hkont": "500000",
        "dmbtr": 2500.00, "waers": "USD", "event_ts": "2024-02-05 11:15:00",
    },

    # Company 2000, document 20000002 (matches FI_2000_20000002_x)
    {
        "belnr": "20000002", "bukrs": "2000", "company_name": "Company 2000",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "240000",
        "dmbtr": 2500.00, "waers": "USD", "event_ts": "2024-02-10 10:00:00",
    },
]

CDC_EVENTS = HARDCODED_CDC_EVENTS
CDC_BACKEND = "local"
CDC_ERROR = None

db_events, err = load_cdc_events_from_databricks()
if db_events:
    CDC_EVENTS = db_events
    CDC_BACKEND = "databricks"
else:
    CDC_BACKEND = "local"
    CDC_ERROR = err

OP_LABELS = {"I": "🟢 INSERT", "U": "🟡 UPDATE", "D": "🔴 DELETE"}


def compute_current_state(events):
    by_key = {}
    for e in events:
        key = (e["belnr"], e["bukrs"], e["gjahr"], e["buzei"])
        if key not in by_key or e["event_ts"] > by_key[key]["event_ts"]:
            by_key[key] = e
    return [e for e in by_key.values() if e["op"] != "D"]


def decorate(events):
    out = []
    for e in events:
        row = dict(e)
        row["op_label"] = OP_LABELS.get(e["op"], e["op"])
        out.append(row)
    return out

# ============================================================================
# 6. Layout – Tabs
# ============================================================================

tab1, tab2 = st.tabs(["CDC Explorer", "RAG Hybrid Search"])

# TAB 1 -----------------------------------------------------------------------
with tab1:
    st.header("CDC Explorer – SLT/ODP Emulation")

    if CDC_BACKEND == "databricks":
        st.success("CDC events loaded from lending_catalog.sap_ai.sap_fi_cdc_events")
    else:
        st.warning(f"Using hard-coded CDC demo events. Reason: {CDC_ERROR}")

    col1, col2 = st.columns(2)
    company = col1.text_input("Company Code", "1000")
    belnr = col2.text_input("Document Number", "")

    rows = [
        e for e in CDC_EVENTS
        if (not company or e["bukrs"] == company)
        and (not belnr or e["belnr"] == belnr)
    ]

    st.subheader("Event History")
    if rows:
        sorted_events = sorted(rows, key=lambda x: x["event_ts"])
        st.dataframe(decorate(sorted_events), use_container_width=True)
    else:
        st.info("No events found.")

    st.subheader("Current CDC State")
    st.dataframe(
        decorate([
            e for e in compute_current_state(CDC_EVENTS)
            if (not company or e["bukrs"] == company)
            and (not belnr or e["belnr"] == belnr)
        ]),
        use_container_width=True
    )

# TAB 2 -----------------------------------------------------------------------
with tab2:
    st.header("RAG Hybrid Search – Docs + SAP FI (Company-Aware)")

    if RAG_BACKEND == "databricks":
        st.success("Loaded corpus from sap_ai.doc_rag_embeddings")
    else:
        st.warning(f"Using fallback corpus. Reason: {RAG_ERROR}")

    question = st.text_area("Ask a question:", height=120)

    col1, col2 = st.columns(2)
    topic = col1.selectbox(
        "Topic",
        ["all", "architecture", "data_vault", "data_quality", "sap_integration", "sap_fi"],
    )
    company_filter = col2.text_input("Company code filter (optional)", "")

    if st.button("Answer"):
        hits = retrieve_chunks(question, topic, company_filter)

        if not hits:
            st.info("No matching documents.")
        else:
            st.subheader("Retrieved Context")
            for i, row in enumerate(hits, start=1):
                label = ""
                if row.get("company_code"):
                    label = f" (Company {row['company_code']} – {row.get('company_name')})"

                st.markdown(
                    f"**{i}. [{row['topic']} / {row['subtopic']}] {label}**\n{row['text']}"
                )

            st.markdown("---")
            st.subheader("Generated Answer (no LLM, raw context)")
            st.write(" ".join(r["text"] for r in hits))
