# Hospital-Operations-Analytics

End-to-End Microsoft Fabric + Power BI ProjectArchitecture
Raw CSV Files
      │
      
BRONZE LAYER    ← Ingest + Metadata + Schema Validation
      │
      
SILVER LAYER    ← Cleanse + Standardize + Business Rules + Quarantine
      │
      
GOLD LAYER      ← Star Schema: Dims + Facts + Aggregates
      │
      
POWER BI        ← 5 Dashboard Pages + 30 DAX Measures + RLS
Project Structure
FolderContents01_raw_data/Synthetic data generator (2K patients, 5K admissions, 10K occupancy)02_bronze/Raw ingestion notebook with schema validation & metadata03_silver/Data cleaning, business rules, quarantine logic04_gold/Star schema: dim_date, dim_patient, fact_admissions, fact_billing05_powerbi_dax/30+ DAX measures with time intelligence & RLS07_pipeline/Fabric Pipeline JSON (Bronze→Silver→Gold→PBI Refresh)06_docs/Full project documentation
Tech Stack

Microsoft Fabric (Lakehouse, Notebooks, Pipelines, Semantic Model)
PySpark — ETL/ELT transformations
Delta Lake — ACID-compliant storage format
Power BI — Dashboards, DAX, RLS
SQL — CTEs, window functions, performance tuning
Azure Data Factory — (Pipeline pattern)

Key Metrics Delivered
KPIValueTotal Admissions Tracked5,000+Revenue Monitored₹51.4 CroreReporting Latency Reduced~60%Data Accuracy~99%Dashboard Pages5DAX Measures30+


How to Run Locally

bash# 1. Generate raw data
cd 01_raw_data && python generate_data.py

# 2. Bronze layer
cd ../02_bronze && python bronze_ingestion.py

# 3. Silver layer
cd ../03_silver && python silver_transformation.py

# 4. Gold layer
cd ../04_gold && python gold_star_schema.py

# 5. Load Gold CSVs into Power BI Desktop
# 6. Import dax_measures.dax into your Measures table

