"""
=============================================================
GOLD LAYER — Star Schema Dimensional Models
Project: Healthcare Analytics Lakehouse
Author: Trinkesh Nimsarkar
Layer: Gold (Silver → Analytics-Ready)
=============================================================
PURPOSE:
  - Build fact & dimension tables (Star Schema)
  - Create aggregated tables for Power BI performance
  - Produce final analytics-ready datasets
  - Semantic model friendly (clean column names, proper keys)

TABLES PRODUCED:
  Dimensions:  dim_patient, dim_date, dim_department, dim_doctor
  Facts:       fact_admissions, fact_billing, fact_bed_occupancy
  Aggregates:  agg_monthly_admissions, agg_dept_kpis, agg_revenue_summary
=============================================================
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, date, timedelta

SILVER_DIR = "../03_silver"
GOLD_DIR   = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────
# DIMENSION: DIM_DATE (calendar table — critical for Power BI)
# ─────────────────────────────────────────────────────────────
def build_dim_date(start="2021-01-01", end="2026-12-31"):
    print("\n── Building: DIM_DATE ──────────────────────────────")
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"]       = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day"]            = df["full_date"].dt.day
    df["month"]          = df["full_date"].dt.month
    df["month_name"]     = df["full_date"].dt.strftime("%B")
    df["month_short"]    = df["full_date"].dt.strftime("%b")
    df["quarter"]        = df["full_date"].dt.quarter
    df["quarter_label"]  = "Q" + df["quarter"].astype(str)
    df["year"]           = df["full_date"].dt.year
    df["year_quarter"]   = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)
    df["year_month"]     = df["full_date"].dt.strftime("%Y-%m")
    df["week_number"]    = df["full_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]    = df["full_date"].dt.dayofweek + 1        # 1=Mon
    df["day_name"]       = df["full_date"].dt.strftime("%A")
    df["is_weekend"]     = df["day_of_week"].isin([6, 7]).astype(int)
    df["is_month_start"] = df["full_date"].dt.is_month_start.astype(int)
    df["is_month_end"]   = df["full_date"].dt.is_month_end.astype(int)
    df["full_date"]      = df["full_date"].dt.strftime("%Y-%m-%d")

    df.to_csv(f"{GOLD_DIR}/dim_date.csv", index=False)
    print(f"   ✅ {len(df):,} date rows | {df['year'].min()} → {df['year'].max()}")
    return df

# ─────────────────────────────────────────────────────────────
# DIMENSION: DIM_PATIENT
# ─────────────────────────────────────────────────────────────
def build_dim_patient():
    print("\n── Building: DIM_PATIENT ───────────────────────────")
    df = pd.read_csv(f"{SILVER_DIR}/silver_patients.csv")

    dim = df[[
        "patient_id", "gender", "date_of_birth", "age", "age_band",
        "blood_group", "city", "insurance_type"
    ]].copy()

    dim.columns = [
        "patient_key", "gender", "date_of_birth", "age", "age_band",
        "blood_group", "city", "insurance_type"
    ]

    dim.to_csv(f"{GOLD_DIR}/dim_patient.csv", index=False)
    print(f"   ✅ {len(dim):,} patients")
    return dim

# ─────────────────────────────────────────────────────────────
# DIMENSION: DIM_DEPARTMENT
# ─────────────────────────────────────────────────────────────
def build_dim_department(admissions_df):
    print("\n── Building: DIM_DEPARTMENT ────────────────────────")
    depts = admissions_df["department"].unique()

    dept_meta = {
        "Cardiology"      : ("Heart & Vascular", "Medical"),
        "Orthopedics"     : ("Bones & Joints",   "Surgical"),
        "Neurology"       : ("Brain & Nerves",   "Medical"),
        "Emergency"       : ("Emergency Care",   "Emergency"),
        "Pediatrics"      : ("Children",         "Medical"),
        "Oncology"        : ("Cancer Care",      "Medical"),
        "General Medicine": ("General",          "Medical"),
        "Icu"             : ("Intensive Care",   "Critical Care"),
        "Gynecology"      : ("Women's Health",   "Surgical"),
        "Urology"         : ("Urinary Tract",    "Surgical"),
    }

    rows = []
    for i, dept in enumerate(sorted(depts), 1):
        meta = dept_meta.get(dept, ("General", "Medical"))
        rows.append({
            "department_key" : i,
            "department_name": dept,
            "specialty"      : meta[0],
            "dept_type"      : meta[1],
        })

    dim = pd.DataFrame(rows)
    dim.to_csv(f"{GOLD_DIR}/dim_department.csv", index=False)
    print(f"   ✅ {len(dim)} departments")
    return dim

# ─────────────────────────────────────────────────────────────
# DIMENSION: DIM_DOCTOR
# ─────────────────────────────────────────────────────────────
def build_dim_doctor(admissions_df):
    print("\n── Building: DIM_DOCTOR ────────────────────────────")
    doctors = admissions_df.groupby("doctor_name")["department"].agg(
        lambda x: x.value_counts().index[0]  # Primary department
    ).reset_index()
    doctors.columns = ["doctor_name", "primary_department"]
    doctors.insert(0, "doctor_key", range(1, len(doctors) + 1))

    doctors.to_csv(f"{GOLD_DIR}/dim_doctor.csv", index=False)
    print(f"   ✅ {len(doctors)} doctors")
    return doctors

# ─────────────────────────────────────────────────────────────
# FACT: FACT_ADMISSIONS (core fact)
# ─────────────────────────────────────────────────────────────
def build_fact_admissions(dim_dept, dim_doctor):
    print("\n── Building: FACT_ADMISSIONS ───────────────────────")
    df = pd.read_csv(f"{SILVER_DIR}/silver_admissions.csv")

    # Join dept key
    dept_map   = dim_dept.set_index("department_name")["department_key"].to_dict()
    doctor_map = dim_doctor.set_index("doctor_name")["doctor_key"].to_dict()

    fact = df.copy()
    fact["department_key"]   = fact["department"].map(dept_map)
    fact["doctor_key"]       = fact["doctor_name"].map(doctor_map)
    fact["admission_date_key"] = pd.to_datetime(fact["admission_date"], errors="coerce").dt.strftime("%Y%m%d").astype("Int64")
    fact["discharge_date_key"] = pd.to_datetime(fact["discharge_date"], errors="coerce").dt.strftime("%Y%m%d")
    fact["discharge_date_key"] = pd.to_numeric(fact["discharge_date_key"], errors="coerce").astype("Int64")

    # Select final fact columns
    fact = fact[[
        "admission_id", "patient_id", "department_key", "doctor_key",
        "admission_date_key", "discharge_date_key",
        "admission_type", "bed_type", "diagnosis_primary",
        "length_of_stay", "los_category", "discharge_status",
        "readmission_flag", "high_risk_flag", "mortality_flag",
        "ward_number", "admission_year", "admission_month", "admission_quarter"
    ]]

    fact.to_csv(f"{GOLD_DIR}/fact_admissions.csv", index=False)
    print(f"   ✅ {len(fact):,} admission facts")
    return fact

# ─────────────────────────────────────────────────────────────
# FACT: FACT_BILLING
# ─────────────────────────────────────────────────────────────
def build_fact_billing():
    print("\n── Building: FACT_BILLING ──────────────────────────")
    df = pd.read_csv(f"{SILVER_DIR}/silver_billing.csv")

    fact = df[[
        "bill_id", "admission_id", "patient_id",
        "total_amount", "room_charges", "medicine_charges",
        "procedure_charges", "misc_charges",
        "insurance_type", "insurance_amount", "patient_payable",
        "outstanding_amount", "payment_status", "revenue_tier",
        "bill_year", "bill_month", "bill_quarter"
    ]].copy()

    fact.to_csv(f"{GOLD_DIR}/fact_billing.csv", index=False)
    print(f"   ✅ {len(fact):,} billing facts  | Total Revenue: ₹{fact['total_amount'].sum():,.0f}")
    return fact

# ─────────────────────────────────────────────────────────────
# FACT: FACT_BED_OCCUPANCY
# ─────────────────────────────────────────────────────────────
def build_fact_bed_occupancy(dim_dept):
    print("\n── Building: FACT_BED_OCCUPANCY ────────────────────")
    df = pd.read_csv(f"{SILVER_DIR}/silver_bed_occupancy.csv")

    dept_map = dim_dept.set_index("department_name")["department_key"].to_dict()
    df["department_key"] = df["department"].map(dept_map)
    df["snapshot_date_key"] = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.strftime("%Y%m%d").astype("Int64")

    fact = df[[
        "occupancy_id", "department_key", "snapshot_date_key",
        "total_beds", "occupied_beds", "available_beds",
        "occupancy_rate", "occupancy_status",
        "snapshot_year", "snapshot_month", "snapshot_quarter"
    ]]

    fact.to_csv(f"{GOLD_DIR}/fact_bed_occupancy.csv", index=False)
    print(f"   ✅ {len(fact):,} occupancy snapshots")
    return fact

# ─────────────────────────────────────────────────────────────
# AGGREGATES (pre-computed for Power BI performance)
# ─────────────────────────────────────────────────────────────
def build_aggregates(fact_adm, fact_bill):
    print("\n── Building: AGGREGATES ────────────────────────────")

    # Agg 1: Monthly Admissions KPIs
    monthly = fact_adm.groupby(["admission_year", "admission_month"]).agg(
        total_admissions   = ("admission_id", "count"),
        avg_los            = ("length_of_stay", "mean"),
        readmissions       = ("readmission_flag", "sum"),
        mortality_count    = ("mortality_flag", "sum"),
        emergency_count    = ("admission_type", lambda x: (x == "Emergency").sum()),
    ).reset_index()
    monthly["readmission_rate"] = (monthly["readmissions"] / monthly["total_admissions"] * 100).round(2)
    monthly["mortality_rate"]   = (monthly["mortality_count"] / monthly["total_admissions"] * 100).round(2)
    monthly["avg_los"]          = monthly["avg_los"].round(1)
    monthly.to_csv(f"{GOLD_DIR}/agg_monthly_admissions.csv", index=False)
    print(f"   ✅ agg_monthly_admissions: {len(monthly)} rows")

    # Agg 2: Department KPIs
    dept_kpis = fact_adm.groupby("department_key").agg(
        total_admissions = ("admission_id", "count"),
        avg_los          = ("length_of_stay", "mean"),
        total_readmit    = ("readmission_flag", "sum"),
        total_mortality  = ("mortality_flag", "sum"),
        high_risk_count  = ("high_risk_flag", "sum"),
    ).reset_index()
    dept_kpis["avg_los"] = dept_kpis["avg_los"].round(1)
    dept_kpis.to_csv(f"{GOLD_DIR}/agg_dept_kpis.csv", index=False)
    print(f"   ✅ agg_dept_kpis: {len(dept_kpis)} departments")

    # Agg 3: Revenue Summary
    revenue = fact_bill.groupby(["bill_year", "bill_month", "insurance_type"]).agg(
        total_revenue      = ("total_amount", "sum"),
        insurance_covered  = ("insurance_amount", "sum"),
        patient_payable    = ("patient_payable", "sum"),
        outstanding        = ("outstanding_amount", "sum"),
        bill_count         = ("bill_id", "count"),
    ).reset_index()
    revenue["collection_rate"] = (
        (revenue["total_revenue"] - revenue["outstanding"]) / revenue["total_revenue"] * 100
    ).round(2)
    revenue.to_csv(f"{GOLD_DIR}/agg_revenue_summary.csv", index=False)
    print(f"   ✅ agg_revenue_summary: {len(revenue)} rows")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🥇 GOLD LAYER — Building Star Schema")
    print("="*60)

    admissions_silver = pd.read_csv(f"{SILVER_DIR}/silver_admissions.csv")

    dim_date   = build_dim_date()
    dim_patient= build_dim_patient()
    dim_dept   = build_dim_department(admissions_silver)
    dim_doctor = build_dim_doctor(admissions_silver)

    fact_adm   = build_fact_admissions(dim_dept, dim_doctor)
    fact_bill  = build_fact_billing()
    fact_occ   = build_fact_bed_occupancy(dim_dept)

    build_aggregates(fact_adm, fact_bill)

    print("\n" + "="*60)
    print("GOLD LAYER — STAR SCHEMA COMPLETE")
    print("="*60)
    print("""
  ┌─────────────────────────────────────────────┐
  │           STAR SCHEMA DIAGRAM               │
  │                                             │
  │   dim_date ──────┐                          │
  │   dim_patient ───┤                          │
  │   dim_department ┼──► fact_admissions       │
  │   dim_doctor ────┘                          │
  │                                             │
  │   dim_date ──────────► fact_billing         │
  │   dim_department ────► fact_bed_occupancy   │
  └─────────────────────────────────────────────┘
    """)
    print("📊 Power BI: Load all gold CSVs → Create relationships on *_key columns")
    print("📊 In Fabric: These become Delta tables in the Lakehouse Gold layer")
