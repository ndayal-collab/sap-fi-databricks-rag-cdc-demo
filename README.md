SAP FI Databricks Lakehouse Architecture

Medallion Architecture • Delta CDC • Data Vault • Documentation Retrieval

This repository implements an SAP FI financial data pipeline on the Databricks Lakehouse platform. It includes ingestion patterns, CDC processing, Data Vault modeling, analytic outputs, and an optional Streamlit UI for exploring CDC behavior and retrieving architecture documentation.

1. Objectives

This solution provides a modular approach for:

Ingesting SAP FI financial documents and line items

Capturing and processing CDC (Insert/Update/Delete) changes

Modeling enterprise data using Data Vault 2.0

Building Business Vault and Gold analytical datasets

Storing architecture documentation as a searchable corpus

Providing an interactive UI for CDC inspection and document retrieval

2. Repository Structure

sap-fi-databricks-rag-cdc-demo/
│
├── databricks/
│ ├── notebooks/ – Ingestion, CDC, Data Vault, Business Vault, Gold
│ ├── jobs/ – Optional job definitions
│
├── docs/
│ ├── Architecture.md
│ ├── CDC_Explorer_Design.md
│ ├── RAG_Corpus_Design.md
│ ├── Streamlit_App_Overview.md
│
├── streamlit_app/
│ ├── streamlit_app.py
│ ├── .env – Databricks credentials (ignored by Git)
│ ├── requirements.txt
│ └── README.md – Instructions for running the Streamlit app
│
└── README.md – This file

3. Data Layers
Bronze Layer

sap_raw — Raw CDC Events
Stores full SAP FI change history:

Insert / Update / Delete events

SAP FI fields (currencies, accounts, amounts)

Business keys (belnr, bukrs, gjahr, buzei)

Event timestamps
Equivalent to an SLT/ODP change queue.

sap_bronze — Current-State Tables
Maintained via Delta MERGE:

Latest image per business key

Delete operations applied

Tracks last operation and last event timestamp

4. Silver Layer — Data Vault

Hubs:

hub_fi_document

hub_gl_account

hub_company

Links:

link_fi_posting

Satellites:

sat_fi_document_header

sat_fi_lineitem_amounts

sat_gl_master_data

sat_company_attributes

Features:

Hash keys and hash diffs

Effective_from / effective_to

Full historization

5. Business Vault

Business Vault enriches the Data Vault by:

Applying business rules

Combining hubs/links/satellites

Adding derived fields

Currency conversions

Producing analytics-friendly structures

6. Gold Layer

Gold tables support analytics and reporting:

fi_fact_posting (atomic fact)

fi_monthly_fact_usd (monthly aggregated fact)

Used for:

Financial analytics

Reconciliation

BI dashboards

ML feature generation

7. Architecture Documentation Corpus (RAG)

Documentation corpus stored in:

lending_catalog.sap_ai.doc_rag_corpus

lending_catalog.sap_ai.doc_rag_embeddings

Fields include:

doc_id

topic

subtopic

text

metadata fields

Streamlit application:

Loads corpus from Databricks when credentials are valid

Falls back to a small in-memory corpus otherwise

Retrieval is keyword-based (no LLM yet)

8. Streamlit Application

Located in streamlit_app/.

Modules:

CDC Explorer:

Displays CDC event history

Computes current-state rows

Mirrors SLT/ODP behavior
Backed by sap_raw.fi_lineitem_events and sap_bronze.fi_lineitem_cdc.

Architecture Q&A (RAG):

Accepts a question

Retrieves relevant architecture text chunks

Works with Databricks or fallback corpus

No LLM generation implemented

Instructions for running the Streamlit app are in streamlit_app/README.md.

9. Delta Lake Features Used

MERGE INTO for CDC upserts

VERSION AS OF for time travel

DESCRIBE HISTORY for lineage

Checkpoints and JSON logs

Partitioning and clustering considerations

10. Extensibility

Possible extensions:

Auto Loader ingestion

SLT/ODP direct connectors

Additional SAP modules (MM, SD, CO)

Embedding-based RAG with vector search

Delta Live Tables or workflow automation

BI integrations (Power BI, dashboards)