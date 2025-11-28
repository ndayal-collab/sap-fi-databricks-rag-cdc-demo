# CDC Explorer Design – SLT/ODP Emulation (Simulated)

This component demonstrates how SAP FI change data capture (CDC) works using a simplified SLT/ODP model. In this version of the project, CDC logic is simulated inside the Streamlit app, but the behavior matches the real Databricks tables that would be used in production.

---

## 1. Purpose

The CDC Explorer helps visualize how SAP FI financial documents change over time. It shows:

- Insert, Update, Delete events
- Full event history
- Latest state per FI line item
- How a CDC merge job works
- How SLT/ODP behaves conceptually

Even though this demo uses simulated events, it is designed to mirror how real SAP FI CDC flows into:

- lending_catalog.sap_raw.fi_lineitem_events
- lending_catalog.sap_bronze.fi_lineitem_cdc

---

## 2. SAP to Databricks CDC Flow (Conceptual)

1. SAP FI source tables (BKPF, BSEG, etc.) produce changes.
2. SLT or ODP captures row-level operations:
   - I (insert)
   - U (update)
   - D (delete)
3. These changes land in a raw Delta table:
   sap_raw.fi_lineitem_events
4. A streaming or scheduled MERGE job processes events into:
   sap_bronze.fi_lineitem_cdc
5. The bronze table contains:
   - Latest image per FI posting (by belnr, bukrs, gjahr, buzei)
   - Last operation
   - All business keys
   - Hard deletes applied when op = D

This provides both full history and current state.

---

## 3. How the Streamlit Demo Works (Simulated)

The Streamlit app uses a small in-memory list:

CDC_EVENTS = [
  { "belnr": "1000001", "bukrs": "1000", "op": "I", ... },
  { "belnr": "1000001", "bukrs": "1000", "op": "U", ... },
  ...
]

Two views are generated:

### 3.1 Event History View
Equivalent to querying sap_raw.fi_lineitem_events:

SELECT *
FROM sap_raw.fi_lineitem_events
ORDER BY event_ts;

The app displays:
- chronological event list
- Insert / Update / Delete labels
- amounts, accounts, currencies
- timeline messages

### 3.2 Current State View
Equivalent to querying sap_bronze.fi_lineitem_cdc:

SELECT *
FROM sap_bronze.fi_lineitem_cdc;

A helper function computes:
- latest event per key
- filter out deletes
- return only surviving rows

This simulates the effect of Delta MERGE.

---

## 4. Why This Mode Exists

Databricks Free Edition does not support SQL Warehouses, so the app cannot query real CDC tables yet.

The simulated mode:
- works offline
- keeps demos consistent
- allows interview explanations
- mirrors real table schema and behavior

---

## 5. Future Enhancement: Connect to Real Tables

When a SQL Warehouse becomes available, the CDC Explorer can be switched to real Databricks tables.

Example adaptation:

SELECT *
FROM lending_catalog.sap_raw.fi_lineitem_events
WHERE bukrs = :bukrs
  AND belnr = :belnr
ORDER BY event_ts;

SELECT *
FROM lending_catalog.sap_bronze.fi_lineitem_cdc
WHERE bukrs = :bukrs
  AND belnr = :belnr;

The rest of the Streamlit UI does not change.

---

## 6. Summary

- The logic is currently simulated but architecturally accurate.
- It directly matches the real raw and bronze CDC tables.
- The demo shows SLT/ODP behavior clearly.
- It is structured so switching to real tables is easy.

This keeps the CDC Explorer reliable in all environments while still demonstrating a proper SAP-to-Databricks CDC flow.
