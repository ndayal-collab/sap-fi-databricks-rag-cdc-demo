# SAP FI Databricks Lakehouse – RAG + CDC Demo

End-to-end demo of a **SAP FI Databricks Lakehouse** that includes:

- Ingestion of SAP FI tables into **Bronze / Vault / Silver / Gold** layers
- **Data Vault 2.0** modeling for FI documents, GL accounts, company codes
- **Validation** notebooks comparing SAP source vs. lakehouse
- **RAG (Retrieval-Augmented Generation)** notebooks over project documentation
- **CDC Emulation** of SAP SLT + ODP queues
- Optional **Streamlit app** for interactive exploration

This repo is structured to be easy to browse on GitHub and easy to import into
a Databricks workspace.

---

## Folder Structure

- `databricks/notebooks/SAP_FI_Lakehouse`
  - `00_Documentation` – glossary, migration strategy, Delta internals notes
  - `01_Ingestion` – create schemas and load BKPF, BSEG, SKA1, T001, TCURR, MARA…
  - `02_Vault` – Data Vault Hubs, Links, Satellites, and BV (business vault) views
  - `03_Silver` – harmonized FI line items and GL enrichment
  - `04_Gold` – star-schema style FI fact tables, monthly facts, comparison views
  - `05_Validation` – source vs lakehouse reconciliation checks
  - `07_RAG` – document corpus, embeddings schema, vector search, QA interface
  - `08_SAP_CDC_Emulation` – SLT event generator and ODP queue consumer notebooks

- `docs`
  - Architecture and design docs for the lakehouse, RAG layer, and CDC emulation.

- `streamlit_app`
  - Optional Streamlit UI for exploring metrics or running sample queries.

---

## How to Use

1. **Prerequisites**

   - Databricks Community Edition or Databricks workspace
   - A cluster with DBR runtime that supports Delta (e.g. 14.x+)

2. **Import notebooks**

   - In Databricks, create a folder e.g. `/Repos/<user>/sap-fi-databricks-rag-cdc-demo`.
   - Sync this GitHub repo into Databricks Repos  
     **or** download and import the `databricks/notebooks/SAP_FI_Lakehouse` tree.

3. **Run the pipeline (high level)**

   1. `01_Ingestion` – run notebooks to create schemas and load SAP FI tables into Bronze.
   2. `02_Vault` – build Data Vault Hubs, Links, and Satellites.
   3. `03_Silver` – create harmonized FI line item and GL-enriched views.
   4. `04_Gold` – build monthly FI fact tables and reporting views.
   5. `05_Validation` – run reconciliation checks.
   6. `08_SAP_CDC_Emulation` – emulate SLT + ODP change data capture.
   7. `07_RAG` – build corpus / embeddings and use the QA notebooks.

4. **Streamlit app (optional)**

   ```bash
   cd streamlit_app
   pip install -r requirements.txt
   streamlit run app.py
