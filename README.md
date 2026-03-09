# 🏥 Hospital Operations Analytics Lakehouse

<div align="center">

![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

**End-to-end healthcare data engineering project built on Microsoft Fabric Medallion Architecture**
*Raw EHR ingestion → Bronze → Silver → Gold → Power BI Production Dashboards*

[![View Project Summary](https://img.shields.io/badge/🌐_View_Live_Project_Summary-006D75?style=for-the-badge)](https://trinkesh-nimsarkar.github.io/hospital-analytics-lakehouse)

</div>

---

## 📌 Overview

A complete **healthcare analytics Lakehouse** solution using Microsoft Fabric. Covers the full data engineering lifecycle — synthetic EHR data generation, multi-layer Medallion pipeline, Star Schema dimensional modeling, and a 5-page Power BI dashboard with 30+ DAX measures and Row Level Security.

> **Domain:** Healthcare Analytics &nbsp;|&nbsp; **Stack:** Microsoft Fabric · Power BI · PySpark · Delta Lake · DAX

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 MICROSOFT FABRIC WORKSPACE                   │
│                                                              │
│  📁 Files/raw/      🥉 BRONZE        🥈 SILVER              │
│  ┌───────────┐     ┌───────────┐    ┌───────────┐           │
│  │ 4 CSV     │────►│ Ingest    │───►│ Cleanse   │           │
│  │ files     │     │ Metadata  │    │ Validate  │           │
│  │ (raw EHR) │     │ Schema ✓  │    │ Quarantine│           │
│  └───────────┘     └───────────┘    └─────┬─────┘           │
│                                           │                  │
│                         🥇 GOLD           │                  │
│                    ┌──────────────┐◄──────┘                  │
│                    │  Star Schema │                          │
│                    │  4 Dims      │                          │
│                    │  3 Facts     │                          │
│                    │  3 Aggs      │                          │
│                    └──────┬───────┘                          │
│                           │                                  │
│                    📊 POWER BI                               │
│                    ┌──────────────┐                          │
│                    │ 5 Dashboards │                          │
│                    │ 30+ Measures │                          │
│                    │ RLS Security │                          │
│                    └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Project Scale

| Metric | Value |
|--------|-------|
| 🧑‍🤝‍🧑 Patients | 2,000 |
| 🏨 Admissions | 5,000 |
| 💰 Revenue Tracked | ₹51.4 Crore |
| 🛏️ Bed Snapshots | 10,950 (daily × 3 years) |
| 📋 Gold Tables | 10 (4 dims + 3 facts + 3 aggs) |
| 📐 DAX Measures | 30+ |
| 📊 Dashboard Pages | 5 |
| ✅ Data Accuracy | ~99% |

---

## 🥉 Bronze Layer — `02_bronze/bronze_ingestion.py`

- Reads 4 CSV files from `Files/raw/` in Fabric Lakehouse
- Adds audit metadata: `_ingestion_timestamp` · `_source_file` · `_layer` · `_record_hash`
- Validates schema — detects missing columns, high null %, duplicate rows
- Saves as **Delta Lake** tables
- Writes quality log to `bronze_ingestion_log.json`

---

## 🥈 Silver Layer — `03_silver/silver_transformation.py`

| Table | Key Transformations |
|-------|-------------------|
| `silver_patients` | DOB parsing, age + age band, gender standardization |
| `silver_admissions` | Dedup, date validation, LOS recalculation, LOS category, risk/mortality flags |
| `silver_billing` | Amount reconciliation, revenue tier, outstanding amount |
| `silver_bed_occupancy` | Occupancy rate, critical/high/moderate/low status |

> Bad records (e.g. discharge before admission) → saved to `quarantine_admissions.csv` with reject reason

---

## 🥇 Gold Layer — `04_gold/gold_star_schema.py`

### Star Schema

```
              dim_date
                 │
   dim_patient   │   dim_department
        │        │        │
        └────────┼────────┘
                 │
           fact_admissions ◄──── dim_doctor
                 │
           fact_billing

   dim_department ──► fact_bed_occupancy ◄── dim_date
```

### Tables

| Table | Type | Rows |
|-------|------|------|
| `fact_admissions` | Fact | 5,000 |
| `fact_billing` | Fact | 5,000 |
| `fact_bed_occupancy` | Fact | 10,950 |
| `dim_patient` | Dimension | 2,000 |
| `dim_date` | Dimension | 2,191 |
| `dim_department` | Dimension | 10 |
| `dim_doctor` | Dimension | 10 |
| `agg_monthly_admissions` | Aggregate | 48 |
| `agg_dept_kpis` | Aggregate | 10 |
| `agg_revenue_summary` | Aggregate | 296 |

---

## 📐 DAX Measures — `05_powerbi_dax/dax_measures.dax`

**Admissions:** `Total Admissions` · `YTD Admissions` · `Admission Growth YoY%` · `Active Admissions` · `Emergency Rate%`

**Clinical Quality:** `Readmission Rate%` · `Readmission vs Benchmark` · `Mortality Rate%` · `High Risk Patients` · `Avg Length of Stay` · `LOS Rolling 3M`

**Revenue:** `Total Revenue` · `YTD Revenue` · `Revenue Growth YoY%` · `Collection Rate%` · `Outstanding Revenue` · `Insurance Coverage%`

**Bed Occupancy:** `Avg Occupancy Rate%` · `Current Occupancy%` · `Available Beds` · `Depts at Critical Capacity`

**Time Intelligence:** `QTD Admissions` · `MoM Revenue Change%` · `Revenue Rolling 12M` · `SAMEPERIODLASTYEAR`

**Dynamic UI:** `Readmission KPI Color` · `Occupancy KPI Color` · `Collection KPI Color` · `Dept Rank by Admissions`

---

## ⚙️ Fabric Pipeline — `07_pipeline/fabric_pipeline_definition.json`

```
[Check Files] → [Bronze ForEach parallel] → [Silver sequential] → [Gold] → [PBI Refresh]
                                                                                  │
                                                                     On Fail: [Email Alert]
```

- **Schedule:** Daily 6:00 AM IST
- **Load:** Incremental (watermark-based `updated_at` filter)
- **Monitoring:** Fabric Monitor Hub

---

## 📊 Dashboard Pages

| Page | Title | Key Visuals |
|------|-------|-------------|
| 1 | Executive Overview | KPI cards, admission trend, revenue trend |
| 2 | Patient Flow | LOS distribution, admission type, dept heatmap |
| 3 | Clinical Quality | Readmission vs benchmark, mortality, high-risk |
| 4 | Revenue & Billing | Revenue by insurance, collection rate, outstanding |
| 5 | Bed Occupancy | Occupancy heatmap, critical alerts, trends |

---

## 📈 Business Impact

| Metric | Before | After |
|--------|--------|-------|
| Reporting Turnaround | 3–5 days (manual) | < 1 hour (automated) |
| Data Accuracy | ~82% | ~99% |
| Pipeline Refresh | 4+ hours | ~45 minutes (−60%) |
| Revenue Visibility | No outstanding tracking | Real-time |
| Readmission Monitoring | None | Live vs 15% benchmark |
| Bed Utilization | Manual daily | Critical capacity alerts |

---

## 📁 Repository Structure

```
hospital-analytics-lakehouse/
├── 01_raw_data/
│   ├── generate_data.py           # Synthetic EHR generator
│   ├── patients_raw.csv
│   ├── admissions_raw.csv
│   ├── billing_raw.csv
│   └── bed_occupancy_raw.csv
├── 02_bronze/
│   └── bronze_ingestion.py        # Raw ingest + metadata + validation
├── 03_silver/
│   └── silver_transformation.py   # Cleansing + rules + quarantine
├── 04_gold/
│   ├── gold_star_schema.py        # Star schema builder
│   └── *.csv                      # All 10 gold tables
├── 05_powerbi_dax/
│   └── dax_measures.dax           # 30+ DAX measures
├── 07_pipeline/
│   └── fabric_pipeline_definition.json
├── 06_docs/
│   └── project_documentation.docx
├── index.html                     # 🌐 Live project summary
└── README.md
```

---

## 🚀 Run Locally

```bash
git clone https://github.com/trinkesh-nimsarkar/hospital-analytics-lakehouse.git
cd hospital-analytics-lakehouse

pip install pandas numpy

python 01_raw_data/generate_data.py
python 02_bronze/bronze_ingestion.py
python 03_silver/silver_transformation.py
python 04_gold/gold_star_schema.py

# Then load Gold CSVs into Power BI Desktop
# Import dax_measures.dax into a Measures table
```

---

## ☁️ Deploy on Microsoft Fabric

1. Create Fabric Workspace → New Lakehouse (`HospitalLakehouse`)
2. Upload raw CSVs to `Files/raw/`
3. Create 3 Notebooks — paste bronze/silver/gold scripts (PySpark equivalents in code comments)
4. Create Data Pipeline — wire notebooks using the JSON as reference
5. Connect Power BI to Lakehouse Gold tables → import DAX → configure RLS
6. Enable scheduled refresh trigger (6 AM IST daily)

---

## 🎓 Certifications

![PL-300](https://img.shields.io/badge/PL--300-Power_BI_Data_Analyst-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![DP-600](https://img.shields.io/badge/DP--600-Fabric_Analytics_Engineering-0078D4?style=flat-square&logo=microsoft&logoColor=white)
![DP-700](https://img.shields.io/badge/DP--700-Fabric_Data_Engineer-0078D4?style=flat-square&logo=microsoft&logoColor=white)

---

## 👤 Author

**Trinkesh Nimsarkar** — Senior BI Developer | Microsoft Fabric Data Engineer

📧 trinkeshn@gmail.com &nbsp;·&nbsp; 💼 [LinkedIn](https://linkedin.com/in/trinkesh-nimsarkar) &nbsp;·&nbsp; 🐱 [GitHub](https://github.com/Trinkesh)

---

<div align="center"><sub>Built with Microsoft Fabric · Power BI · PySpark · Delta Lake · DAX · SQL</sub></div>
