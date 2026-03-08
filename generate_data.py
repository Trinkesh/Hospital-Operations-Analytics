"""
=============================================================
HOSPITAL OPERATIONS - RAW DATA GENERATOR
Project: Healthcare Analytics Lakehouse
Author: Trinkesh Nimsarkar
Layer: Raw / Landing Zone (Pre-Bronze)
=============================================================
Generates realistic synthetic hospital data mimicking real 
EHR exports. Produces 4 CSV files that simulate source system dumps.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# ── Seed for reproducibility ──────────────────────────────
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Reference Data ────────────────────────────────────────
DEPARTMENTS    = ["Cardiology", "Orthopedics", "Neurology", "Emergency", "Pediatrics",
                  "Oncology", "General Medicine", "ICU", "Gynecology", "Urology"]
DOCTORS        = [f"Dr. {n}" for n in [
                  "Sharma", "Mehta", "Patel", "Iyer", "Reddy",
                  "Joshi", "Kulkarni", "Singh", "Verma", "Nair"]]
DIAGNOSIS      = ["Hypertension", "Diabetes Type 2", "Fracture", "Appendicitis",
                  "Pneumonia", "Stroke", "Cancer - Stage II", "Kidney Stone",
                  "Heart Attack", "Dengue", "COVID-19", "Sepsis", "Anemia", "Asthma"]
INSURANCE      = ["Star Health", "HDFC Ergo", "New India", "Bajaj Allianz", "Self-Pay", "Government"]
ADMISSION_TYPE = ["Emergency", "Planned", "Referral"]
DISCHARGE_TYPE = ["Recovered", "LAMA", "Death", "Referred", "Absconded"]
BED_TYPES      = ["General", "Semi-Private", "Private", "ICU", "HDU"]
CITIES         = ["Nagpur", "Mumbai", "Pune", "Delhi", "Hyderabad", "Chennai", "Bangalore"]
GENDERS        = ["Male", "Female"]

def random_date(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

# ─────────────────────────────────────────────────────────
# TABLE 1: PATIENTS  (dim-like master)
# ─────────────────────────────────────────────────────────
def generate_patients(n=2000):
    records = []
    for i in range(1, n + 1):
        dob = random_date(1940, 2005)
        records.append({
            "patient_id"    : f"PAT{i:05d}",
            "first_name"    : f"Patient_{i}",
            "last_name"     : f"Surname_{random.randint(1,500)}",
            "gender"        : random.choice(GENDERS),
            "date_of_birth" : dob.strftime("%Y-%m-%d"),
            "blood_group"   : random.choice(["A+","A-","B+","B-","O+","O-","AB+","AB-"]),
            "city"          : random.choice(CITIES),
            "insurance_type": random.choice(INSURANCE),
            "contact_number": f"9{random.randint(100000000,999999999)}",
            "created_at"    : random_date(2021, 2022).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(records)

# ─────────────────────────────────────────────────────────
# TABLE 2: ADMISSIONS  (fact - core)
# ─────────────────────────────────────────────────────────
def generate_admissions(patients_df, n=5000):
    records = []
    patient_ids = patients_df["patient_id"].tolist()

    for i in range(1, n + 1):
        admit_date    = random_date(2022, 2025)
        los           = random.randint(1, 45)            # length of stay (days)
        discharge_date= admit_date + timedelta(days=los)
        dept          = random.choice(DEPARTMENTS)
        bed_type      = "ICU" if dept == "ICU" else random.choice(BED_TYPES[:4])
        discharge_type= random.choices(DISCHARGE_TYPE, weights=[70,10,5,10,5])[0]

        # Introduce ~3% NULLs in discharge date (still admitted)
        if random.random() < 0.03:
            discharge_date = None
            discharge_type = None

        records.append({
            "admission_id"    : f"ADM{i:06d}",
            "patient_id"      : random.choice(patient_ids),
            "department"      : dept,
            "doctor_name"     : random.choice(DOCTORS),
            "admission_type"  : random.choice(ADMISSION_TYPE),
            "admission_date"  : admit_date.strftime("%Y-%m-%d"),
            "discharge_date"  : discharge_date.strftime("%Y-%m-%d") if discharge_date else None,
            "length_of_stay"  : los if discharge_date else None,
            "bed_type"        : bed_type,
            "diagnosis_primary": random.choice(DIAGNOSIS),
            "discharge_status": discharge_type,
            "readmission_flag": random.choices([0, 1], weights=[85, 15])[0],
            "ward_number"     : f"W{random.randint(1,20):02d}",
            "created_at"      : admit_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at"      : (admit_date + timedelta(hours=random.randint(1,100))).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(records)

# ─────────────────────────────────────────────────────────
# TABLE 3: BILLING  (fact - financials)
# ─────────────────────────────────────────────────────────
def generate_billing(admissions_df):
    records = []
    for _, row in admissions_df.iterrows():
        if row["discharge_date"] is None:
            continue  # Still admitted, bill not generated

        base_charge = random.uniform(5000, 200000)
        insurance   = row.get("insurance_type", "Self-Pay") if "insurance_type" in row else "Self-Pay"
        insurance_pct = {"Star Health":0.8,"HDFC Ergo":0.75,"New India":0.7,
                         "Bajaj Allianz":0.65,"Self-Pay":0,"Government":0.9}.get(insurance, 0)

        insurance_amount = round(base_charge * insurance_pct, 2)
        patient_amount   = round(base_charge - insurance_amount, 2)

        records.append({
            "bill_id"           : f"BILL{len(records)+1:07d}",
            "admission_id"      : row["admission_id"],
            "patient_id"        : row["patient_id"],
            "bill_date"         : row["discharge_date"],
            "total_amount"      : round(base_charge, 2),
            "room_charges"      : round(base_charge * 0.35, 2),
            "medicine_charges"  : round(base_charge * 0.25, 2),
            "procedure_charges" : round(base_charge * 0.30, 2),
            "misc_charges"      : round(base_charge * 0.10, 2),
            "insurance_type"    : insurance,
            "insurance_amount"  : insurance_amount,
            "patient_payable"   : patient_amount,
            "payment_status"    : random.choices(["Paid","Pending","Partial","Waived"],
                                                  weights=[65,20,10,5])[0],
            "created_at"        : row["discharge_date"],
        })
    return pd.DataFrame(records)

# ─────────────────────────────────────────────────────────
# TABLE 4: BED OCCUPANCY LOG  (operational fact - daily)
# ─────────────────────────────────────────────────────────
def generate_bed_occupancy():
    records = []
    record_id = 1
    for dept in DEPARTMENTS:
        total_beds = random.randint(20, 80)
        start = datetime(2022, 1, 1)
        for day_offset in range(0, 365 * 3, 1):        # 3 years daily
            snapshot_date = start + timedelta(days=day_offset)
            occupied      = random.randint(int(total_beds * 0.4), total_beds)
            records.append({
                "occupancy_id"   : f"OCC{record_id:07d}",
                "department"     : dept,
                "snapshot_date"  : snapshot_date.strftime("%Y-%m-%d"),
                "total_beds"     : total_beds,
                "occupied_beds"  : occupied,
                "available_beds" : total_beds - occupied,
                "occupancy_rate" : round(occupied / total_beds * 100, 2),
                "created_at"     : snapshot_date.strftime("%Y-%m-%d 23:00:00"),
            })
            record_id += 1
    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────
# MAIN — Generate & Save
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🏥 Generating Hospital Dataset...")

    patients   = generate_patients(2000)
    admissions = generate_admissions(patients, 5000)

    # Enrich admissions with insurance for billing join
    admissions_enriched = admissions.merge(
        patients[["patient_id","insurance_type"]], on="patient_id", how="left")

    billing    = generate_billing(admissions_enriched)
    occupancy  = generate_bed_occupancy()

    # Save
    patients.to_csv(f"{OUTPUT_DIR}/patients_raw.csv",       index=False)
    admissions.to_csv(f"{OUTPUT_DIR}/admissions_raw.csv",   index=False)
    billing.to_csv(f"{OUTPUT_DIR}/billing_raw.csv",         index=False)
    occupancy.to_csv(f"{OUTPUT_DIR}/bed_occupancy_raw.csv", index=False)

    print(f"✅ patients_raw.csv       → {len(patients):,} rows")
    print(f"✅ admissions_raw.csv     → {len(admissions):,} rows")
    print(f"✅ billing_raw.csv        → {len(billing):,} rows")
    print(f"✅ bed_occupancy_raw.csv  → {len(occupancy):,} rows")
    print(f"\n📁 Files saved to: {OUTPUT_DIR}")
