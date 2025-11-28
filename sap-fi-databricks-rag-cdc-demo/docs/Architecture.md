# SAP FI Databricks Architecture Overview
Medallion Lakehouse + Data Vault + CDC + RAG

This document explains the technical architecture used to implement SAP FI ingestion, CDC processing, Data Vault modeling, and retrieval-based documentation search on Databricks.

---

# 1. End-to-End Architecture

The solution follows a layered, modular structure:

1. Landing (optional)
2. Bronze (Raw + Enhanced)
3. Silver (Data Vault hubs, links, satellites)
4. Business Vault models
5. Gold analytic fact tables
6. Architecture corpus for RAG-based documentation lookup
7. Streamlit application for visualization and Q&A

This architecture is aligned with Databricks Lakehouse patterns and SAP financial data characteristics.

---

# 2. Landing Layer

This layer accepts extracted SAP data or synthetic data.

Key properties:
- Data is stored exactly as received
- No transformations
- Supports files, events, or API outputs
- Suitable for AutoLoader or direct uploads

This layer is optional when synthetic pipeline data is used.

---

# 3. Bronze Layer

The Bronze layer consists of two sublayers that represent the state of SAP change capture.

## 3.1 Raw Bronze: sap_raw

Raw Bronze stores the CDC event log:

Table: sap_raw.fi_lineitem_events

Characteristics:
- Contains all Insert (I), Update (U), and Delete (D) events
- Includes event timestamps
- Preserves SAP keys (belnr, bukrs, gjahr, buzei)
- Retains the financial line-item attributes
- Represents the full historical change feed

This table is conceptually similar to:
- an SAP ODP delta queue
- an SLT change table
- a Kafka topic of row-level operations

## 3.2 Enhanced Bronze: sap_bronze

Enhanced Bronze stores the current-state view:

Table: sap_bronze.fi_lineitem_cdc

Properties:
- Holds the latest image per business key
- Maintains last operation (insert/update/delete)
- Implements hard deletes for D operations
- Maintained via MERGE INTO logic

This table is suitable for downstream processing and modeling.

---

# 4. Silver Layer – Data Vault

The Silver layer implements a structured, auditable Data Vault 2.0 model for SAP FI.

## 4.1 Hubs  
Represent business keys:

- hub_fi_document (belnr, bukrs, gjahr)
- hub_gl_account (hkont)
- hub_company (bukrs)

## 4.2 Links  
Represent associations:

- link_fi_posting (documents ↔ accounts)

## 4.3 Satellites  
Carry descriptive and historical attributes:

- sat_fi_document_header
- sat_fi_lineitem_amounts
- sat_gl_master_data
- sat_company_attributes

Key characteristics:
- Full historization
- Hash keys and hash diffs
- Effective timestamp ranges
- Audit control fields

This model absorbs SAP schema changes without breaking downstream consumers.

---

# 5. Business Vault

Business Vault logic enhances the Data Vault output with derivations and business rules.

Examples:
- joined FI posting view
- currency conversions
- debit/credit classifications
- enriched master data attributes

These models serve as the foundation for the Gold layer.

---

# 6. Gold Layer – Analytics

The Gold layer provides atomic and aggregated FI facts.

Examples:
- fi_fact_posting
- fi_monthly_fact_usd

These tables support:
- reporting
- analytics
- financial reconciliation
- onward modeling

Gold tables reflect transformed, curated, analytics-ready data structures.

---

# 7. Architecture Corpus (RAG)

The architecture corpus stores documentation as searchable text chunks.

Location:
lending_catalog.sap_ai.doc_rag_corpus

Columns:
- doc_id
- topic
- subtopic
- text

This enables:
- centralized technical documentation
- retrieval of relevant architecture descriptions
- explainability of the pipeline’s components

If Databricks connectivity is unavailable, the system uses an in-app fallback corpus.

---

# 8. Streamlit Application Layers

The application provides two modules:

## 8.1 CDC Explorer  
Shows:
- historical change events
- current-state computation
- event timelines

Simulates behavior of:
sap_raw.fi_lineitem_events  
sap_bronze.fi_lineitem_cdc

## 8.2 Architecture Q&A  
Provides:
- question input
- retrieval of relevant documentation chunks
- context-based answers

Uses either:
- Databricks corpus (primary)
- local fallback corpus (secondary)

---

# 9. Delta Lake Features

The architecture leverages multiple Delta capabilities:

- MERGE INTO for current-state CDC
- table versioning
- DESCRIBE HISTORY for audit and lineage
- snapshot reads via VERSION AS OF
- checkpoint and log file mechanics
- partitioning strategies by date or company code

These capabilities support reliable change processing and historization.

---

# 10. Extensibility

The architecture allows for:

- direct integration with SAP SLT or ODP
- AutoLoader ingestion
- expansion to additional SAP modules
- embedding-based RAG
- full CDC job orchestration
- notebook-to-pipeline conversion

The structure supports scalable expansion without redesign.

---

# 11. Summary

This architecture demonstrates a full SAP FI ingestion and modeling pipeline built using Databricks Lakehouse patterns, Data Vault storage, Delta Lake CDC handling, and a documentation retrieval system. Each layer is modular, auditable, and designed to handle SAP financial data structures efficiently.
