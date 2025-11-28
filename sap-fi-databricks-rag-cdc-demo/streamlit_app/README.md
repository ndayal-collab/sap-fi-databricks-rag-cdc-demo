# Streamlit Application
SAP FI Databricks Lakehouse – CDC Explorer and Architecture Q&A

This application provides two main capabilities:

1. CDC Explorer  
   - Displays Insert/Update/Delete event history  
   - Computes current-state CDC rows  
   - Shows a timeline of operations  
   - Mirrors SLT/ODP-style behavior  

2. Architecture Q&A (RAG)  
   - Accepts a user question  
   - Retrieves relevant architecture documentation chunks  
   - Uses Databricks corpus if available, otherwise falls back to local corpus  

---------------------------------------------------------------------

# 1. Prerequisites

- Python 3.10+
- A virtual environment (recommended)
- A Databricks Workspace + SQL Warehouse (optional but supported)
- The `.env` file placed inside this folder if using Databricks connectivity

---------------------------------------------------------------------

# 2. Install Dependencies

Create and activate a virtual environment:

Windows:
    python -m venv .venv
    .venv\Scripts\activate

Mac/Linux:
    python3 -m venv .venv
    source .venv/bin/activate

Install required packages:

    pip install -r requirements.txt

---------------------------------------------------------------------

# 3. Databricks Connectivity (Optional)

If you want the Architecture Q&A tab to load documentation from Databricks:

Create a `.env` file inside this folder:

    DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com
    DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
    DATABRICKS_TOKEN=your_token_here

The app will automatically detect and use these credentials.

If any variable is missing, the application switches to a local fallback mode.

---------------------------------------------------------------------

# 4. Running the Application

Run:

    streamlit run app.py

The application will start at:

    http://localhost:8501

---------------------------------------------------------------------

# 5. Application Modes

### Online Mode
- Uses Databricks SQL Warehouse
- Loads corpus from:
  lending_catalog.sap_ai.doc_rag_corpus

### Offline Mode
- No Databricks connection required
- Uses a small local in-memory corpus
- CDC Explorer still operates normally

---------------------------------------------------------------------

# 6. File Structure

This folder contains:

- app.py               Main Streamlit app
- requirements.txt     Python dependencies
- .env                 Databricks credentials (not committed)
- README.md            This file

---------------------------------------------------------------------

# 7. Notes

- No data is stored locally.  
- The application contains no SAP source data.  
- CDC logic is simulated to match expected SLT/ODP behavior.  
- All architecture documentation is either retrieved from Databricks or from a structured in-memory corpus.
