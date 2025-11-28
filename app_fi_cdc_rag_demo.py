import streamlit as st

st.set_page_config(page_title="SAP FI CDC + RAG Demo", layout="wide")

st.title("SAP FI CDC + RAG Demo")

tab1, tab2 = st.tabs(["CDC Explorer", "Architecture Q&A"])

with tab1:
    st.header("CDC Explorer – SLT/ODP Emulation")
    st.write("This tab will connect to Databricks and show CDC behaviour.")
    company = st.text_input("Filter by Company Code (BUKRS)", "")
    belnr = st.text_input("Filter by Document Number (BELNR)", "")
    st.info("TODO: query sap_bronze.fi_lineitem_cdc and sap_raw.fi_lineitem_events")

with tab2:
    st.header("Architecture Q&A – RAG over doc_rag_corpus")
    question = st.text_area(
        "Ask a question about the SAP → Databricks → CDC → DV → RAG architecture:",
        height=120,
        placeholder="e.g., How did we emulate SLT/ODP CDC in this POC?",
    )

    if st.button("Answer"):
        if not question.strip():
            st.warning("Please enter a question.")
        else:
            st.info("TODO: fetch relevant chunks from sap_ai.doc_rag_corpus and show them here.")
