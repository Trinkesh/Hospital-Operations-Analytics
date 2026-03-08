"""
=============================================================
BRONZE LAYER — Raw Ingestion & Metadata Tagging
Project: Healthcare Analytics Lakehouse
Author: Trinkesh Nimsarkar
Layer: Bronze (Raw → Ingested)
=============================================================
In Microsoft Fabric: This runs as a Fabric Notebook (PySpark).
Locally: Runs with pandas to simulate the same logic.

PURPOSE:
  - Ingest raw CSV files from Landing Zone
  - Add audit/metadata columns
  - Detect and log schema issues
  - Save as Delta-like Parquet (Delta Lake in Fabric)
  - NO business transformations at this layer
=============================================================
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────
RAW_DIR    = "../01_raw_data"
BRONZE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE   = f"{BRONZE_DIR}/bronze_ingestion_log.json"

# In Microsoft Fabric, replace with:
# RAW_DIR    = "Files/raw/"
# BRONZE_DIR = "Tables/bronze/"
# spark.read.csv(...)  →  delta format

# ── Metadata columns added at Bronze layer ────────────────
def add_metadata(df: pd.DataFrame, source_file: str, layer: str = "bronze") -> pd.DataFrame:
    df = df.copy()
    df["_ingestion_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["_source_file"]         = source_file
    df["_layer"]               = layer
    df["_record_hash"]         = df.apply(
        lambda row: str(abs(hash(str(row.values.tolist())))), axis=1
    )
    return df

# ── Schema Validation ─────────────────────────────────────
def validate_schema(df: pd.DataFrame, expected_cols: list, table_name: str) -> dict:
    missing  = [c for c in expected_cols if c not in df.columns]
    extra    = [c for c in df.columns if c not in expected_cols]
    null_pct = {c: round(df[c].isnull().mean() * 100, 2) for c in df.columns}
    dup_count= df.duplicated().sum()

    report = {
        "table"          : table_name,
        "row_count"      : len(df),
        "column_count"   : len(df.columns),
        "missing_columns": missing,
        "extra_columns"  : extra,
        "null_percentages": null_pct,
        "duplicate_rows" : int(dup_count),
        "status"         : "PASS" if not missing else "WARN",
    }
    return report

# ── Expected Schemas ─────────────────────────────────────
SCHEMAS = {
    "patients": ["patient_id","first_name","last_name","gender","date_of_birth",
                 "blood_group","city","insurance_type","contact_number","created_at"],
    "admissions": ["admission_id","patient_id","department","doctor_name","admission_type",
                   "admission_date","discharge_date","length_of_stay","bed_type",
                   "diagnosis_primary","discharge_status","readmission_flag","ward_number",
                   "created_at","updated_at"],
    "billing": ["bill_id","admission_id","patient_id","bill_date","total_amount",
                "room_charges","medicine_charges","procedure_charges","misc_charges",
                "insurance_type","insurance_amount","patient_payable","payment_status","created_at"],
    "bed_occupancy": ["occupancy_id","department","snapshot_date","total_beds",
                      "occupied_beds","available_beds","occupancy_rate","created_at"],
}

# ── Main Ingestion ─────────────────────────────────────────
def ingest_to_bronze():
    logs = []
    tables = {
        "patients"     : "patients_raw.csv",
        "admissions"   : "admissions_raw.csv",
        "billing"      : "billing_raw.csv",
        "bed_occupancy": "bed_occupancy_raw.csv",
    }

    for table_name, filename in tables.items():
        src_path = os.path.join(RAW_DIR, filename)
        print(f"\n{'='*60}")
        print(f"📥 Ingesting: {filename}")

        # ── READ ──────────────────────────────────────────
        df = pd.read_csv(src_path, low_memory=False)
        print(f"   Rows: {len(df):,}  |  Cols: {len(df.columns)}")

        # ── VALIDATE ──────────────────────────────────────
        report = validate_schema(df, SCHEMAS[table_name], table_name)
        logs.append(report)

        if report["missing_columns"]:
            print(f"   ⚠️  Missing cols: {report['missing_columns']}")
        if report["duplicate_rows"] > 0:
            print(f"   ⚠️  Duplicates found: {report['duplicate_rows']}")

        # ── ADD METADATA ──────────────────────────────────
        df = add_metadata(df, filename, "bronze")

        # ── SAVE AS PARQUET (Delta in Fabric) ─────────────
        out_path = os.path.join(BRONZE_DIR, f"bronze_{table_name}.parquet")
        df.to_parquet(out_path, index=False, engine="pyarrow")
        print(f"   ✅ Saved → bronze_{table_name}.parquet")

        # Show null summary for key columns
        high_null = {k: v for k, v in report["null_percentages"].items() if v > 5}
        if high_null:
            print(f"   📊 High null cols: {high_null}")

    # ── SAVE LOG ──────────────────────────────────────────
    with open(LOG_FILE, "w") as f:
        json.dump(logs, f, indent=2)
    print(f"\n📋 Ingestion log saved → bronze_ingestion_log.json")

    # ── SUMMARY ───────────────────────────────────────────
    print("\n" + "="*60)
    print("BRONZE INGESTION SUMMARY")
    print("="*60)
    for r in logs:
        status_icon = "✅" if r["status"] == "PASS" else "⚠️"
        print(f"{status_icon} {r['table']:<20} | {r['row_count']:>6,} rows | {r['duplicate_rows']} dups | Status: {r['status']}")

# ── Fabric PySpark Equivalent (commented reference) ───────
FABRIC_PYSPARK_EQUIVALENT = """
# ── Microsoft Fabric Notebook (PySpark) equivalent ────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, sha2, concat_ws, col, lit
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Read from Fabric Lakehouse Files section
df = spark.read.option("header", True).option("inferSchema", True) \\
    .csv("Files/raw/admissions_raw.csv")

# Add metadata
df = df \\
    .withColumn("_ingestion_timestamp", current_timestamp()) \\
    .withColumn("_source_file", input_file_name()) \\
    .withColumn("_layer", lit("bronze")) \\
    .withColumn("_record_hash", sha2(concat_ws("||", *df.columns), 256))

# Write to Delta Lake (Bronze table in Fabric Lakehouse)
df.write.format("delta") \\
    .mode("append") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("bronze_admissions")

print("Bronze layer written to Delta Lake ✅")
"""

if __name__ == "__main__":
    import pyarrow  # noqa — ensure pyarrow available
    ingest_to_bronze()
    print("\n💡 NOTE: In Microsoft Fabric, replace pandas with PySpark + Delta Lake.")
    print("   See FABRIC_PYSPARK_EQUIVALENT variable in this file for reference code.")
