from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("nhs-waitinglist-transform") \
        .getOrCreate()


def load_all_csvs(spark: SparkSession, raw_dir: str):
    """Load all monthly CSVs into a single Spark DataFrame."""
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{raw_dir}/*.csv")


def clean_column_names(df):
    """Standardise column names to snake_case."""
    renamed = {
        "Period": "period",
        "Org Code": "org_code",
        "Parent Org": "parent_org",
        "Org name": "org_name",
        "A&E attendances Type 1": "ae_attendances_type1",
        "A&E attendances Type 2": "ae_attendances_type2",
        "A&E attendances Other A&E Department": "ae_attendances_other",
        "A&E attendances Booked Appointments Type 1": "booked_appts_type1",
        "A&E attendances Booked Appointments Type 2": "booked_appts_type2",
        "A&E attendances Booked Appointments Other Department": "booked_appts_other",
        "Attendances over 4hrs Type 1": "over_4hrs_type1",
        "Attendances over 4hrs Type 2": "over_4hrs_type2",
        "Attendances over 4hrs Other Department": "over_4hrs_other",
        "Attendances over 4hrs Booked Appointments Type 1": "over_4hrs_booked_type1",
        "Attendances over 4hrs Booked Appointments Type 2": "over_4hrs_booked_type2",
        "Attendances over 4hrs Booked Appointments Other Department": "over_4hrs_booked_other",
        "Patients who have waited 4-12 hs from DTA to admission": "wait_4_12hrs_dta",
        "Patients who have waited 12+ hrs from DTA to admission": "wait_12plus_hrs_dta",
        "Emergency admissions via A&E - Type 1": "emergency_admissions_type1",
        "Emergency admissions via A&E - Type 2": "emergency_admissions_type2",
        "Emergency admissions via A&E - Other A&E department": "emergency_admissions_other",
        "Other emergency admissions": "other_emergency_admissions",
    }
    for old, new in renamed.items():
        df = df.withColumnRenamed(old, new)
    return df


def add_derived_columns(df):
    """Add calculated metrics useful for analysis."""
    df = df.withColumn(
        "total_attendances",
        F.col("ae_attendances_type1") + F.col("ae_attendances_type2") + F.col("ae_attendances_other")
    )
    df = df.withColumn(
        "total_over_4hrs",
        F.col("over_4hrs_type1") + F.col("over_4hrs_type2") + F.col("over_4hrs_other")
    )
    df = df.withColumn(
        "breach_rate_pct",
        F.round(
            (F.col("total_over_4hrs") / F.col("total_attendances") * 100).cast(DoubleType()), 2
        )
    )
    df = df.withColumn(
        "total_emergency_admissions",
        F.col("emergency_admissions_type1") + F.col("emergency_admissions_type2") + F.col("emergency_admissions_other")
    )
    return df


def remove_nulls(df):
    """Drop rows where org_code or period is null."""
    return df.dropna(subset=["org_code", "period"])


def save_as_parquet(df, dest_dir: str):
    """Save transformed data as Parquet."""
    os.makedirs(dest_dir, exist_ok=True)
    output_path = f"{dest_dir}/nhs_ae_transformed"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved to {output_path}")


def run():
    print("Starting PySpark transform...")
    spark = create_spark_session()

    df = load_all_csvs(spark, RAW_DIR)
    print(f"Loaded {df.count()} rows from raw CSVs.")

    df = clean_column_names(df)
    df = remove_nulls(df)
    df = add_derived_columns(df)

    print("Sample output:")
    df.select("period", "org_name", "total_attendances", "total_over_4hrs", "breach_rate_pct").show(5)

    save_as_parquet(df, PROCESSED_DIR)
    print("Transform complete.")


if __name__ == "__main__":
    run()