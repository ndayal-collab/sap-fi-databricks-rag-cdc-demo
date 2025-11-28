# SAP FI Databricks Lakehouse Architecture
Medallion Architecture • Data Vault • Delta CDC • Documentation Retrieval

This repository contains a structured implementation of an SAP FI financial data pipeline on the Databricks Lakehouse platform. It includes ingestion patterns, CDC processing, Data Vault modeling, analytic tables, and an optional Streamlit interface for exploring CDC behavior and searching architecture documentation.

---------------------------------------------------------------------

# 1. Objectives

The solution provides a modular approach for:

- Ingesting SAP FI financial documents and line items
- Capturing and processing CDC (Insert/Update/Delete) changes
- Modeling enterprise data using Data Vault 2.0
- Producing analytic-ready fact tables
- Storing architecture documentation as a searchable corpus
- Providing an interactive UI for CDC inspection and architecture Q&A

The design follows Databricks Lakehouse best practices and SAP financial data structures.

---------------------------------------------------------------------

# 2. Repository Structure

sap-fi-databricks-rag-cdc-demo/
│
├── databricks/
│   ├── notebooks/        # SQL + Python notebooks for ingestion, CDC, DV, BV, Gold
│   ├── jobs/             # optional job definitions or scripts
│
├── docs/
│   ├── Architecture.md
│   ├── CDC_Explorer_Design.md
│   ├── RAG_Corpus_Design.md
│   ├── Streamlit_App_Overview.md
│
├── streamlit_app/
│   ├── app.py
│   ├── .env              # Databricks credentials (not committed)
│   ├── requirements.txt
│   └── README.md
│
└── README.md

---------------------------------------------------------------------

# 3. Data Layers

## Bronze Layer (Raw + Enhanced)

### sap_raw (Raw Events)
Stores full CDC history:
- Insert / Update / Delete events
- Event timestamps
- SAP FI fields (amounts, currencies, accounts)
- Business keys (belnr, bukrs, gjahr, buzei)

Equivalent to an SAP ODP/SLT change queue.

### sap_bronze (Current State)
Maintained via Delta MERGE:
- Latest image per key
- Hard deletes applied
- Tracks last operation

---------------------------------------------------------------------

# 4. Silver Layer – Data Vault

## Hubs  
Represent business keys.
- hub_fi_document  
- hub_gl_account  
- hub_company  

## Links  
Associations.
- link_fi_posting  

## Satellites  
Historical descriptive attributes.
- sat_fi_document_header  
- sat_fi_lineitem_amounts  
- sat_gl_master_data  
- sat_company_attributes  

Characteristics:
- Hash keys / hash diffs
- Full historization
- Effective_from / effective_to

---------------------------------------------------------------------

# 5. Business Vault

Business Vault enriches the DV layer by:
- Joining hubs, links, satellites
- Adding derived fields
- Applying business rules
- Performing currency conversions
- Preparing analytics-friendly outputs

---------------------------------------------------------------------

# 6. Gold Layer – Analytics

Gold tables provide analytic fact structures:
- fi_fact_posting (atomic)
- fi_monthly_fact_usd (aggregated)

Used for:
- reporting
- analytics
- financial reconciliation
- API output models

---------------------------------------------------------------------

# 7. Architecture Documentation Corpus (RAG)

A searchable corpus is stored in:

lending_catalog.sap_ai.doc_rag_corpus

Columns:
- doc_id
- topic
- subtopic
- text
- optional metadata fields

The Streamlit application:
- Loads live corpus when Databricks credentials are available
- Falls back to local corpus when offline

Retrieval uses keyword-overlap scoring.

---------------------------------------------------------------------

# 8. Streamlit Application

Located in streamlit_app/.

Modules:

## CDC Explorer
- Displays event history
- Computes current-state CDC rows
- Shows operation timelines
- Simulates SLT/ODP behavior

Reflects patterns of:
- sap_raw.fi_lineitem_events
- sap_bronze.fi_lineitem_cdc

## Architecture Q&A
- Accepts a user question
- Retrieves relevant architecture chunks
- Returns concatenated context

Works in:
- Online mode (Databricks corpus)
- Offline mode (local corpus)

---------------------------------------------------------------------

# 9. Running the Streamlit App

1. Create a virtual environment

   python -m venv .venv
   .venv\Scripts\activate    (Windows)
   source .venv/bin/activate (Mac/Linux)

2. Install dependencies

   pip install -r streamlit_app/requirements.txt

3. Create `.env` inside streamlit_app/

   DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
   DATABRICKS_TOKEN=your_token

4. Launch the app

   streamlit run streamlit_app/app.py

---------------------------------------------------------------------

# 10. Delta Lake Features Used

- MERGE INTO for CDC state
- DESCRIBE HISTORY for commit lineage
- VERSION AS OF for snapshots
- Partitioning considerations
- JSON log + checkpoint structure

These features enable reliable processing of SAP financial changes.

---------------------------------------------------------------------

# 11. Extensibility

The architecture can be extended with:

- SAP SLT or ODP direct connections
- AutoLoader ingestion pipelines
- Expanded SAP module support
- Embedding-powered RAG
- Delta live pipelines or workflows
- Dashboarding or BI integrations

---------------------------------------------------------------------

# 12. License

This repository does not include license restrictions. Add a LICENSE file if required.

