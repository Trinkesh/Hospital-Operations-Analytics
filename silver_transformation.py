"""
=============================================================
SILVER LAYER — Data Cleaning, Standardization & Enrichment
Project: Healthcare Analytics Lakehouse
Author: Trinkesh Nimsarkar
Layer: Silver (Bronze → Cleansed)
=============================================================
In Microsoft Fabric: This runs as a Fabric Notebook (PySpark).
Locally: Runs with pandas to simulate the same logic.

PURPOSE:
  - Remove duplicates
  - Standardize formats (dates, strings, categories)
  - Apply business rules & data quality checks
  - Derive calculated columns (age, LOS category, risk flags)
  - Handle NULLs with documented rules
  - Flag bad/rejected records (quarantine)
=============================================================
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

BRONZE_DIR = "../02_bronze"
SILVER_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Quality Report tracker ─────────────────────────────────
quality_report = {}

def log_quality(table, stage, before, after):
    if table not in quality_report:
        quality_report[table] = []
    quality_report[table].append({
        "stage": stage,
        "before": before,
        "after": after,
        "dropped": before - after
    })

# =============================================================
# SILVER: PATIENTS
# =============================================================
def transform_patients():
    print("\n── Transforming: PATIENTS ──────────────────────────")
    df = pd.read_csv(f"{BRONZE_DIR}/bronze_patients.csv")
    initial_count = len(df)

    # 1. Remove exact duplicates
    df = df.drop_duplicates(subset=["patient_id"])
    log_quality("patients", "dedup_patient_id", initial_count, len(df))

    # 2. Standardize gender
    df["gender"] = df["gender"].str.strip().str.title()
    df = df[df["gender"].isin(["Male", "Female", "Other"])]

    # 3. Parse & validate dates
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    df = df[df["date_of_birth"].notna()]  # Drop invalid DOBs
    log_quality("patients", "valid_dob", initial_count, len(df))

    # 4. Derive AGE
    today = pd.Timestamp.today()
    df["age"] = ((today - df["date_of_birth"]).dt.days / 365.25).astype(int)

    # 5. Age band (for Power BI slicers)
    bins   = [0, 18, 35, 50, 65, 200]
    labels = ["0-18", "19-35", "36-50", "51-65", "65+"]
    df["age_band"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)

    # 6. Standardize insurance
    df["insurance_type"] = df["insurance_type"].str.strip().fillna("Unknown")

    # 7. Standardize city
    df["city"] = df["city"].str.strip().str.title()

    # 8. Add silver metadata
    df["_silver_processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_silver_valid"]        = True

    df.to_csv(f"{SILVER_DIR}/silver_patients.csv", index=False)
    print(f"   ✅ {len(df):,} valid patients saved")
    return df

# =============================================================
# SILVER: ADMISSIONS
# =============================================================
def transform_admissions():
    print("\n── Transforming: ADMISSIONS ────────────────────────")
    df = pd.read_csv(f"{BRONZE_DIR}/bronze_admissions.csv")
    initial_count = len(df)

    # 1. Dedup on admission_id
    df = df.drop_duplicates(subset=["admission_id"])
    log_quality("admissions", "dedup_admission_id", initial_count, len(df))

    # 2. Parse dates
    df["admission_date"]  = pd.to_datetime(df["admission_date"],  errors="coerce")
    df["discharge_date"]  = pd.to_datetime(df["discharge_date"],  errors="coerce")

    # 3. Business Rule: discharge cannot be before admission
    invalid_dates = df["discharge_date"] < df["admission_date"]
    quarantine_df = df[invalid_dates].copy()
    quarantine_df["_reject_reason"] = "discharge_before_admission"
    df = df[~invalid_dates]
    log_quality("admissions", "valid_date_order", initial_count, len(df))

    # 4. Recalculate LOS (length of stay) from dates — more reliable than source
    df["length_of_stay"] = (df["discharge_date"] - df["admission_date"]).dt.days
    # For still-admitted patients (NULL discharge), LOS is days since admission
    still_admitted = df["discharge_date"].isna()
    df.loc[still_admitted, "length_of_stay"] = (
        pd.Timestamp.today() - df.loc[still_admitted, "admission_date"]
    ).dt.days

    # 5. LOS Category
    def los_category(los):
        if pd.isna(los): return "Unknown"
        if los <= 1:     return "Same Day"
        if los <= 3:     return "Short (1-3d)"
        if los <= 7:     return "Medium (4-7d)"
        if los <= 14:    return "Long (8-14d)"
        return "Extended (15d+)"

    df["los_category"] = df["length_of_stay"].apply(los_category)

    # 6. Extract date parts for Power BI calendar joins
    df["admission_year"]  = df["admission_date"].dt.year
    df["admission_month"] = df["admission_date"].dt.month
    df["admission_month_name"] = df["admission_date"].dt.strftime("%b")
    df["admission_quarter"] = df["admission_date"].dt.quarter
    df["admission_week"]  = df["admission_date"].dt.isocalendar().week.astype(int)

    # 7. Standardize text fields
    df["department"]       = df["department"].str.strip().str.title()
    df["doctor_name"]      = df["doctor_name"].str.strip().str.title()
    df["admission_type"]   = df["admission_type"].str.strip().str.title()
    df["diagnosis_primary"]= df["diagnosis_primary"].str.strip().str.title()
    df["discharge_status"] = df["discharge_status"].fillna("Active")

    # 8. Risk flag: ICU + readmission = high risk
    df["high_risk_flag"] = (
        (df["bed_type"] == "ICU") | (df["readmission_flag"] == 1)
    ).astype(int)

    # 9. Mortality flag
    df["mortality_flag"] = (df["discharge_status"] == "Death").astype(int)

    # 10. Save valid + quarantine
    df["_silver_processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_silver_valid"]        = True

    df.to_csv(f"{SILVER_DIR}/silver_admissions.csv", index=False)
    if len(quarantine_df) > 0:
        quarantine_df.to_csv(f"{SILVER_DIR}/quarantine_admissions.csv", index=False)
        print(f"   ⚠️  {len(quarantine_df)} records quarantined")

    print(f"   ✅ {len(df):,} valid admissions saved")
    return df

# =============================================================
# SILVER: BILLING
# =============================================================
def transform_billing():
    print("\n── Transforming: BILLING ───────────────────────────")
    df = pd.read_csv(f"{BRONZE_DIR}/bronze_billing.csv")
    initial_count = len(df)

    # 1. Dedup
    df = df.drop_duplicates(subset=["bill_id"])
    log_quality("billing", "dedup_bill_id", initial_count, len(df))

    # 2. Business Rule: component charges must sum near total_amount
    df["_calc_total"] = (
        df["room_charges"] + df["medicine_charges"] +
        df["procedure_charges"] + df["misc_charges"]
    )
    df["_amount_variance"] = abs(df["total_amount"] - df["_calc_total"])
    df["_amount_check"]    = df["_amount_variance"] < 1  # within ₹1

    # 3. Nulls: fill payment_status unknown
    df["payment_status"] = df["payment_status"].fillna("Unknown")

    # 4. Revenue classification
    def revenue_tier(amount):
        if amount < 10000:  return "Low (<10K)"
        if amount < 50000:  return "Medium (10K-50K)"
        if amount < 100000: return "High (50K-1L)"
        return "Premium (>1L)"

    df["revenue_tier"] = df["total_amount"].apply(revenue_tier)

    # 5. Outstanding amount (pending/partial)
    df["outstanding_amount"] = np.where(
        df["payment_status"] == "Paid", 0,
        np.where(df["payment_status"] == "Partial",
                 df["patient_payable"] * 0.5,
                 df["patient_payable"])
    )

    # 6. Parse bill_date
    df["bill_date"] = pd.to_datetime(df["bill_date"], errors="coerce")
    df["bill_year"]    = df["bill_date"].dt.year
    df["bill_month"]   = df["bill_date"].dt.month
    df["bill_quarter"] = df["bill_date"].dt.quarter

    df["_silver_processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(f"{SILVER_DIR}/silver_billing.csv", index=False)
    print(f"   ✅ {len(df):,} billing records saved")
    return df

# =============================================================
# SILVER: BED OCCUPANCY
# =============================================================
def transform_bed_occupancy():
    print("\n── Transforming: BED OCCUPANCY ─────────────────────")
    df = pd.read_csv(f"{BRONZE_DIR}/bronze_bed_occupancy.csv")

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df = df[df["snapshot_date"].notna()]

    # Validate occupancy_rate
    df["occupancy_rate"] = df["occupied_beds"] / df["total_beds"] * 100
    df["occupancy_rate"] = df["occupancy_rate"].round(2)

    # Occupancy status
    def occ_status(rate):
        if rate >= 95: return "Critical"
        if rate >= 80: return "High"
        if rate >= 60: return "Moderate"
        return "Low"

    df["occupancy_status"] = df["occupancy_rate"].apply(occ_status)
    df["snapshot_year"]    = df["snapshot_date"].dt.year
    df["snapshot_month"]   = df["snapshot_date"].dt.month
    df["snapshot_quarter"] = df["snapshot_date"].dt.quarter

    df["_silver_processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(f"{SILVER_DIR}/silver_bed_occupancy.csv", index=False)
    print(f"   ✅ {len(df):,} occupancy records saved")
    return df

# =============================================================
# MAIN
# =============================================================
if __name__ == "__main__":
    print("🥈 SILVER LAYER — Starting Transformations")
    print("="*60)

    patients   = transform_patients()
    admissions = transform_admissions()
    billing    = transform_billing()
    occupancy  = transform_bed_occupancy()

    print("\n" + "="*60)
    print("SILVER LAYER QUALITY SUMMARY")
    print("="*60)
    for table, stages in quality_report.items():
        print(f"\n  📋 {table.upper()}")
        for s in stages:
            print(f"     {s['stage']:<35} Before: {s['before']:>6,}  After: {s['after']:>6,}  Dropped: {s['dropped']}")

    # ── Fabric PySpark Reference ───────────────────────────
    FABRIC_SILVER_PYSPARK = """
    # ── Silver transformation in Fabric (PySpark) ──────────
    from pyspark.sql.functions import *
    from pyspark.sql.types import *

    df = spark.table("bronze_admissions")

    df_silver = df \\
        .dropDuplicates(["admission_id"]) \\
        .withColumn("admission_date", to_date(col("admission_date"), "yyyy-MM-dd")) \\
        .withColumn("discharge_date", to_date(col("discharge_date"), "yyyy-MM-dd")) \\
        .withColumn("length_of_stay", datediff(col("discharge_date"), col("admission_date"))) \\
        .withColumn("los_category",
            when(col("length_of_stay") <= 1, "Same Day")
            .when(col("length_of_stay") <= 3, "Short (1-3d)")
            .when(col("length_of_stay") <= 7, "Medium (4-7d)")
            .otherwise("Long (8d+)")) \\
        .withColumn("high_risk_flag",
            when((col("bed_type") == "ICU") | (col("readmission_flag") == 1), 1).otherwise(0)) \\
        .withColumn("_silver_processed_at", current_timestamp())

    df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_admissions")
    """
    print("\n💡 Fabric PySpark equivalent saved in comments within this file.")
