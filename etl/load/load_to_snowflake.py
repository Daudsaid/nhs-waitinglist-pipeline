import snowflake.connector
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = "data/processed/nhs_ae_transformed"


def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def create_table(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('SNOWFLAKE_DATABASE')}")
    cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {os.getenv('SNOWFLAKE_SCHEMA')}")
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nhs_ae_attendances (
            period                      VARCHAR,
            org_code                    VARCHAR,
            parent_org                  VARCHAR,
            org_name                    VARCHAR,
            ae_attendances_type1        NUMBER,
            ae_attendances_type2        NUMBER,
            ae_attendances_other        NUMBER,
            booked_appts_type1          NUMBER,
            booked_appts_type2          NUMBER,
            booked_appts_other          NUMBER,
            over_4hrs_type1             NUMBER,
            over_4hrs_type2             NUMBER,
            over_4hrs_other             NUMBER,
            over_4hrs_booked_type1      NUMBER,
            over_4hrs_booked_type2      NUMBER,
            over_4hrs_booked_other      NUMBER,
            wait_4_12hrs_dta            NUMBER,
            wait_12plus_hrs_dta         NUMBER,
            emergency_admissions_type1  NUMBER,
            emergency_admissions_type2  NUMBER,
            emergency_admissions_other  NUMBER,
            other_emergency_admissions  NUMBER,
            total_attendances           NUMBER,
            total_over_4hrs             NUMBER,
            breach_rate_pct             FLOAT,
            total_emergency_admissions  NUMBER
        )
    """)
    print("Table ready.")


def clean_row(row):
    """Convert numpy types and NaN to Python native types for Snowflake."""
    cleaned = []
    for val in row:
        if isinstance(val, float) and np.isnan(val):
            cleaned.append(None)
        elif isinstance(val, np.integer):
            cleaned.append(int(val))
        elif isinstance(val, np.floating):
            cleaned.append(float(val))
        else:
            cleaned.append(val)
    return tuple(cleaned)


def load_parquet_to_snowflake(cursor, conn):
    df = pd.read_parquet(PROCESSED_DIR)
    print(f"Loaded {len(df)} rows from Parquet.")

    cursor.execute("TRUNCATE TABLE nhs_ae_attendances")

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    rows = [clean_row(row) for row in df.itertuples(index=False, name=None)]

    cursor.executemany(
        f"INSERT INTO nhs_ae_attendances ({cols}) VALUES ({placeholders})",
        rows
    )
    conn.commit()
    print(f"Inserted {len(rows)} rows into Snowflake.")


def run():
    print("Connecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    create_table(cursor)
    load_parquet_to_snowflake(cursor, conn)

    cursor.close()
    conn.close()
    print("Load complete.")


if __name__ == "__main__":
    run()
