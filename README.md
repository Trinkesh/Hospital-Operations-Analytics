#  Hospital Operations Analytics Lakehouse

**End-to-End Microsoft Fabric + Power BI Project**  
*By Trinkesh Nimsarkar — Senior BI Developer | Microsoft Fabric Data Engineer*

---

## Project Summary

A full healthcare analytics solution using **Microsoft Fabric Medallion Architecture** — from raw EHR data ingestion to production Power BI dashboards with 30+ KPIs.

## Architecture

```
Raw CSV Files
      │
      
 BRONZE LAYER    ← Ingest + Metadata + Schema Validation
      │
      
SILVER LAYER    ← Cleanse + Standardize + Business Rules + Quarantine
      │
      
GOLD LAYER      ← Star Schema: Dims + Facts + Aggregates
      │
      ▼
POWER BI        ← 5 Dashboard Pages + 30 DAX Measures + RLS
```

## Project Structure

| Folder | Contents |
|--------|----------|
| `01_raw_data/` | Synthetic data generator (2K patients, 5K admissions, 10K occupancy) |
| `02_bronze/` | Raw ingestion notebook with schema validation & metadata |
| `03_silver/` | Data cleaning, business rules, quarantine logic |
| `04_gold/` | Star schema: dim_date, dim_patient, fact_admissions, fact_billing |
| `05_powerbi_dax/` | 30+ DAX measures with time intelligence & RLS |
| `07_pipeline/` | Fabric Pipeline JSON (Bronze→Silver→Gold→PBI Refresh) |
| `06_docs/` | Full project documentation |

##  Tech Stack

- **Microsoft Fabric** (Lakehouse, Notebooks, Pipelines, Semantic Model)  
- **PySpark** — ETL/ELT transformations  
- **Delta Lake** — ACID-compliant storage format  
- **Power BI** — Dashboards, DAX, RLS  
- **SQL** — CTEs, window functions, performance tuning  
- **Azure Data Factory** — (Pipeline pattern)  

## Key Metrics Delivered

| KPI | Value |
|-----|-------|
| Total Admissions Tracked | 5,000+ |
| Revenue Monitored | ₹51.4 Crore |
| Reporting Latency Reduced | ~60% |
| Data Accuracy | ~99% |
| Dashboard Pages | 5 |
| DAX Measures | 30+ |

## Certifications Applied

- **PL-300** — Power BI Data Analyst  
- **DP-600** — Fabric Analytics Engineering Associate  
- **DP-700** — Fabric Data Engineer Associate  

##  How to Run Locally

```bash
# 1. Generate raw data
cd 01_raw_data && python generate_data.py

# 2. Bronze layer
cd ../02_bronze && python bronze_ingestion.py

# 3. Silver layer
cd ../03_silver && python silver_transformation.py

# 4. Gold layer
cd ../04_gold && python gold_star_schema.py

# 5. Load Gold CSVs into Power BI Desktop
# 6. Import dax_measures.dax into your Measures table
```

## Contact

**Trinkesh Nimsarkar** | trinkeshn@gmail.com | [LinkedIn](https://linkedin.com/in/trinkesh-nimsarkar)
