import os
import re
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# 1. Load environment variables from .env next to this script
# ------------------------------------------------------------------------------

APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# ------------------------------------------------------------------------------
# 2. Page config
# ------------------------------------------------------------------------------

st.set_page_config(page_title="SAP FI CDC + RAG Demo", layout="wide")
st.title("SAP FI CDC + RAG Demo")

# ------------------------------------------------------------------------------
# 3. Show Databricks env-var status (nothing more)
# ------------------------------------------------------------------------------

if DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN:
    st.sidebar.success("Databricks env vars detected (HOST, HTTP_PATH, TOKEN).")
else:
    st.sidebar.warning(
        "Databricks env vars not fully set.\n\n"
        "Expected:\n"
        "- DATABRICKS_HOST\n"
        "- DATABRICKS_HTTP_PATH\n"
        "- DATABRICKS_TOKEN\n\n"
        "RAG will fall back to the local demo corpus."
    )

# ------------------------------------------------------------------------------
# 4. Try loading Databricks SQL connector
# ------------------------------------------------------------------------------

try:
    from databricks import sql as dbsql
    HAS_DBSQL = True
except Exception:
    HAS_DBSQL = False

# ------------------------------------------------------------------------------
# 5. RAG corpus – local fallback + optional Databricks load
# ------------------------------------------------------------------------------

FALLBACK_DOC_RAG_CORPUS = [
    {
        "doc_id": "ARCH_OVERVIEW_001",
        "topic": "architecture",
        "subtopic": "sap_to_databricks",
        "text": (
            "In this SAP–Databricks proof of concept, we simulate SAP FI source tables "
            "(BKPF and BSEG) and land them in a Lakehouse using the Medallion pattern."
        ),
    },
    {
        "doc_id": "ARCH_OVERVIEW_001",
        "topic": "architecture",
        "subtopic": "sap_to_databricks",
        "text": (
            "The goal is to mirror how an SAP ECC or S/4HANA system feeds a Lakehouse "
            "with auditability, historization, and clear lineage."
        ),
    },
    # … keep your other local items here …
]


def load_corpus_from_databricks():
    """Try to load RAG corpus from sap_ai.doc_rag_corpus. Return (rows, error_msg)."""
    if not HAS_DBSQL:
        return None, "databricks-sql-connector not installed"

    if not (DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN):
        return None, "Databricks env vars not fully set"

    try:
        conn = dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
        cursor = conn.cursor()

        cursor.execute("""
    SELECT doc_id, topic, subtopic, text
    FROM lending_catalog.sap_ai.doc_rag_corpus
    LIMIT 500
""")


        rows = [
            {"doc_id": r[0], "topic": r[1], "subtopic": r[2], "text": r[3]}
            for r in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        if not rows:
            return None, "sap_ai.doc_rag_corpus returned zero rows"

        return rows, None

    except Exception as e:
        return None, f"Databricks error: {e}"


DOC_RAG_CORPUS = FALLBACK_DOC_RAG_CORPUS
RAG_BACKEND = "local_demo"
error_msg = "not loaded"

live_data, error_msg = load_corpus_from_databricks()
if live_data:
    DOC_RAG_CORPUS = live_data
    RAG_BACKEND = "databricks"

# ------------------------------------------------------------------------------
# 6. Simple keyword retrieval
# ------------------------------------------------------------------------------

def retrieve_chunks(question: str, top_k: int = 3):
    q_tokens = set(re.findall(r"\w+", question.lower()))
    scored = []

    for row in DOC_RAG_CORPUS:
        t_tokens = set(re.findall(r"\w+", row["text"].lower()))
        score = len(q_tokens & t_tokens)
        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for score, row in scored[:top_k]]

# ------------------------------------------------------------------------------
# 7. CDC events (still simulated for now)
# ------------------------------------------------------------------------------

CDC_EVENTS = [
    {
        "belnr": "1000001", "bukrs": "1000", "company_name": "Contoso Finance AG",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "400000",
        "dmbtr": 1000.00, "waers": "EUR", "event_ts": "2024-01-15 09:00:00",
    },
    {
        "belnr": "1000001", "bukrs": "1000", "company_name": "Contoso Finance AG",
        "gjahr": 2024, "buzei": 1, "op": "U", "hkont": "400000",
        "dmbtr": 1200.00, "waers": "EUR", "event_ts": "2024-01-15 10:15:00",
    },
    {
        "belnr": "1000001", "bukrs": "1000", "company_name": "Contoso Finance AG",
        "gjahr": 2024, "buzei": 1, "op": "U", "hkont": "400000",
        "dmbtr": 1150.00, "waers": "EUR", "event_ts": "2024-01-16 14:30:00",
    },
    {
        "belnr": "1000002", "bukrs": "1000", "company_name": "Contoso Finance AG",
        "gjahr": 2024, "buzei": 1, "op": "I", "hkont": "110000",
        "dmbtr": 2000.00, "waers": "USD", "event_ts": "2024-02-01 08:45:00",
    },
    {
        "belnr": "1000002", "bukrs": "1000", "company_name": "Contoso Finance AG",
        "gjahr": 2024, "buzei": 1, "op": "D", "hkont": "110000",
        "dmbtr": 2000.00, "waers": "USD", "event_ts": "2024-02-05 11:10:00",
    },
]

OP_LABELS = {
    "I": "🟢 INSERT",
    "U": "🟡 UPDATE",
    "D": "🔴 DELETE",
}


def compute_current_state(events):
    by_key = {}
    for e in events:
        key = (e["belnr"], e["bukrs"], e["gjahr"], e["buzei"])
        if key not in by_key or e["event_ts"] > by_key[key]["event_ts"]:
            by_key[key] = e
    return [e for e in by_key.values() if e["op"] != "D"]


def decorate_events(events):
    decorated = []
    for e in events:
        row = dict(e)
        row["op_label"] = OP_LABELS.get(e["op"], e["op"])
        decorated.append(row)
    return decorated

# ------------------------------------------------------------------------------
# 8. Layout – Tabs
# ------------------------------------------------------------------------------

tab1, tab2 = st.tabs(["CDC Explorer", "Architecture Q&A"])

# TAB 1 ------------------------------------------------------------------------
with tab1:
    st.header("CDC Explorer – SLT/ODP Emulation")

    col1, col2 = st.columns(2)
    with col1:
        company = st.text_input("Company Code (BUKRS)", "1000")
    with col2:
        belnr = st.text_input("Document Number (BELNR)", "1000001")

    filtered_events = [
        e for e in CDC_EVENTS
        if (not company or e["bukrs"] == company)
        and (not belnr or e["belnr"] == belnr)
    ]

    st.subheader("Event History (sap_raw.fi_lineitem_events)")
    if not filtered_events:
        st.info("No events found.")
    else:
        sorted_events = sorted(filtered_events, key=lambda e: e["event_ts"])
        st.dataframe(decorate_events(sorted_events), use_container_width=True)

        st.markdown("#### Timeline")
        for e in sorted_events:
            st.markdown(
                f"- **{e['event_ts']}** – {OP_LABELS[e['op']]} {e['dmbtr']} {e['waers']} "
                f"(HKONT {e['hkont']}, {e['company_name']})"
            )

    st.subheader("Current-State CDC (sap_bronze.fi_lineitem_cdc)")
    state_rows = compute_current_state(CDC_EVENTS)
    state_filtered = [
        e for e in state_rows
        if (not company or e["bukrs"] == company)
        and (not belnr or e["belnr"] == belnr)
    ]

    if not state_filtered:
        st.warning("This record is deleted or has no CDC state.")
    else:
        st.dataframe(decorate_events(state_filtered), use_container_width=True)

# TAB 2 ------------------------------------------------------------------------
with tab2:
    st.header("Architecture Q&A – RAG over doc_rag_corpus")

    if RAG_BACKEND == "databricks":
        st.success("Using RAG corpus from sap_ai.doc_rag_corpus.")
    else:
        st.info(f"Using local fallback corpus. Reason: {error_msg}")

    question = st.text_area(
        "Ask a question about the SAP → Databricks → CDC → DV → RAG architecture:",
        height=120,
    )

    if st.button("Answer"):
        if not question.strip():
            st.warning("Enter a question.")
        else:
            hits = retrieve_chunks(question)
            if not hits:
                st.info("No relevant context found.")
            else:
                st.subheader("Relevant Retrieved Context")
                for i, row in enumerate(hits, start=1):
                    st.markdown(
                        f"**{i}. [{row['topic']} / {row['subtopic']}]**\n{row['text']}"
                    )
                st.markdown("---")
                st.markdown("Generated answer (stitched from context):")
                st.write(" ".join([h["text"] for h in hits]))
