import psycopg2
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

PROCESSED_DIR = "data/processed/nhs_ae_transformed"

EXPECTED_COLUMNS = [
    "period", "org_code", "parent_org", "org_name",
    "ae_attendances_type1", "ae_attendances_type2", "ae_attendances_other",
    "booked_appts_type1", "booked_appts_type2", "booked_appts_other",
    "over_4hrs_type1", "over_4hrs_type2", "over_4hrs_other",
    "over_4hrs_booked_type1", "over_4hrs_booked_type2", "over_4hrs_booked_other",
    "wait_4_12hrs_dta", "wait_12plus_hrs_dta",
    "emergency_admissions_type1", "emergency_admissions_type2", "emergency_admissions_other",
    "other_emergency_admissions",
    "total_attendances", "total_over_4hrs", "breach_rate_pct", "total_emergency_admissions"
]


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="nhs_waiting_db",
        user="daudsaid",
        password=""
    )


def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nhs_ae_attendances (
            period                      VARCHAR,
            org_code                    VARCHAR,
            parent_org                  VARCHAR,
            org_name                    VARCHAR,
            ae_attendances_type1        NUMERIC,
            ae_attendances_type2        NUMERIC,
            ae_attendances_other        NUMERIC,
            booked_appts_type1          NUMERIC,
            booked_appts_type2          NUMERIC,
            booked_appts_other          NUMERIC,
            over_4hrs_type1             NUMERIC,
            over_4hrs_type2             NUMERIC,
            over_4hrs_other             NUMERIC,
            over_4hrs_booked_type1      NUMERIC,
            over_4hrs_booked_type2      NUMERIC,
            over_4hrs_booked_other      NUMERIC,
            wait_4_12hrs_dta            NUMERIC,
            wait_12plus_hrs_dta         NUMERIC,
            emergency_admissions_type1  NUMERIC,
            emergency_admissions_type2  NUMERIC,
            emergency_admissions_other  NUMERIC,
            other_emergency_admissions  NUMERIC,
            total_attendances           NUMERIC,
            total_over_4hrs             NUMERIC,
            breach_rate_pct             FLOAT,
            total_emergency_admissions  NUMERIC
        )
    """)
    print("Table ready.")


def clean_row(row):
    cleaned = []
    for val in row:
        if val is None:
            cleaned.append(None)
        elif isinstance(val, float) and np.isnan(val):
            cleaned.append(None)
        elif isinstance(val, np.integer):
            cleaned.append(int(val))
        elif isinstance(val, np.floating):
            cleaned.append(float(val))
        else:
            cleaned.append(val)
    return tuple(cleaned)


def load_parquet_to_postgres(cursor, conn):
    df = pd.read_parquet(PROCESSED_DIR)
    print(f"Loaded {len(df)} rows from Parquet.")

    existing_cols = [c for c in EXPECTED_COLUMNS if c in df.columns]
    df = df[existing_cols]

    cursor.execute("TRUNCATE TABLE nhs_ae_attendances")

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    rows = [clean_row(row) for row in df.itertuples(index=False, name=None)]

    cursor.executemany(
        f"INSERT INTO nhs_ae_attendances ({cols}) VALUES ({placeholders})",
        rows
    )
    conn.commit()
    print(f"Inserted {len(rows)} rows into PostgreSQL.")


def run():
    print("Connecting to PostgreSQL...")
    conn = get_connection()
    cursor = conn.cursor()

    create_table(cursor)
    load_parquet_to_postgres(cursor, conn)

    cursor.close()
    conn.close()
    print("Load complete.")


if __name__ == "__main__":
    run()
