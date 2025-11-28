-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01_SLT_FI_Events_Schema
-- MAGIC
-- MAGIC This notebook defines the Delta table that emulates SAP SLT (row-based CDC)
-- MAGIC for FI line items.
-- MAGIC
-- MAGIC Conceptually, we treat this table as a **Kafka topic** of change events:
-- MAGIC
-- MAGIC - Each row is a single event
-- MAGIC - Events carry operation CRUD type (I/U/D) and FI keys (BELNR, BUKRS, GJAHR, BUZEI)
-- MAGIC - The full row is stored as JSON in `payload`, similar to a Kafka message value
-- MAGIC
-- MAGIC Later notebooks will:
-- MAGIC - Generate synthetic events (insert, update, delete) into this table
-- MAGIC - Consume these events with Structured Streaming
-- MAGIC - Apply them to a Bronze CDC table that feeds Data Vault and Silver/Gold layers.
-- MAGIC

-- COMMAND ----------

-- Databricks notebook source
USE CATALOG lending_catalog;

-- Schema for raw SAP-style ingestion and CDC emulation
CREATE SCHEMA IF NOT EXISTS sap_raw;
USE SCHEMA sap_raw;

-- SLT-style FI line item events "topic"
CREATE TABLE IF NOT EXISTS fi_lineitem_events (
    event_id      BIGINT GENERATED ALWAYS AS IDENTITY,  -- event sequence
    op            STRING,        -- 'I' (insert), 'U' (update), 'D' (delete)
    belnr         STRING,        -- document number (BKPF/BSEG key)
    bukrs         STRING,        -- company code
    gjahr         INT,           -- fiscal year
    buzei         STRING,        -- line item number
    payload       STRING,        -- JSON payload of the FI row
    event_ts      TIMESTAMP      -- when the event "arrived" in the queue
);


-- COMMAND ----------

USE CATALOG lending_catalog;
USE SCHEMA sap_raw;

DESCRIBE TABLE fi_lineitem_events;
