-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_ai;

INSERT INTO doc_rag_corpus (
    doc_id,
    topic,
    subtopic,
    chunk_index,
    text,
    source,
    last_updated
)
VALUES
-- 1. CDC pattern overview
(
  'CDC_PATTERN_001',
  'sap_integration',
  'slt_odp_cdc',
  1,
  'The SAP FI CDC path emulates SLT/ODP using two main tables. sap_raw.fi_lineitem_events behaves like a Kafka/ODP queue: it stores all Insert, Update, and Delete events for FI line items, with JSON payloads and event timestamps. A streaming Delta job reads these events and MERGEs them into sap_bronze.fi_lineitem_cdc, which holds the latest image of each FI line item keyed by (belnr, bukrs, gjahr, buzei). Hard deletes are applied for op = D, while Inserts and Updates upsert the current state.',
  '01_ODP_FI_CDC_Consumer',
  current_timestamp()
),

-- 2. How to inspect inserts / updates / deletes
(
  'CDC_PATTERN_001',
  'sap_integration',
  'slt_odp_cdc',
  2,
  'There are three levels to inspect CDC behaviour. First, the current-state table sap_bronze.fi_lineitem_cdc shows the latest image per FI line item: SELECT last_op, COUNT(*) FROM sap_bronze.fi_lineitem_cdc GROUP BY last_op; tells how many rows ended with Insert vs Update. Second, the raw event table sap_raw.fi_lineitem_events stores the full I/U/D history and can be queried by key (belnr, bukrs, gjahr, buzei) ordered by event_ts to see the full change sequence. Third, Delta time travel on sap_bronze.fi_lineitem_cdc (using DESCRIBE HISTORY and VERSION AS OF) allows you to reconstruct earlier snapshots of the CDC table and see what it looked like before later updates or deletes.',
  '01_ODP_FI_CDC_Consumer',
  current_timestamp()
);
