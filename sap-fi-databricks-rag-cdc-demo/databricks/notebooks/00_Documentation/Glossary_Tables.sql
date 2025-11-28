-- Databricks notebook source
# SAP Table Glossary

## Finance (FI)

### BKPF — Document Header
Contains top-level information for each FI document.  
Key fields: BELNR (document number), BUKRS (company code), GJAHR (fiscal year)

### BSEG — Line Items
Stores individual line items that belong to FI documents.  
Key fields: BELNR, BUZEI (line number), HKONT (account), DMBTR (amount)

### T001 — Company Code Master
Defines company-level attributes such as local currency, name, and country.

### TCURR — Exchange Rates
Provides valid-from FX rates between currency pairs.

### SKA1 — G/L Master
Contains basic attributes for G/L accounts including account description and group.

---

## Materials (MM)

### MARA — Material Master (General)
Material type, material group, unit of measure, and description.

---

## Silver Models (Cleansed)

### fi_lineitem
Join of BSEG + BKPF + T001.  
Produces analysis-ready FI line items with header and company information.

### fi_lineitem_conv
Adds currency conversion using TCURR.

### fi_lineitem_enriched
Adds G/L descriptions and group from SKA1.

---

## Gold Models (Analytics)

### fact_fi_monthly_grp_usd
Financial fact aggregated by company, account and month, expressed in USD.
