# NHS A&E Waiting List Pipeline

An end-to-end data engineering pipeline ingesting NHS England A&E attendance and waiting time data, transforming it with PySpark, loading into Snowflake, and modelling with dbt.

## Architecture

```
NHS England (CSV) → Python Extract → PySpark Transform → Snowflake → dbt Models → Airflow Orchestration
```

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, BeautifulSoup, Requests |
| Transformation | PySpark |
| Warehouse | Snowflake |
| Modelling | dbt (Snowflake adapter) |
| Orchestration | Apache Airflow (Docker) |

## Dataset

- **Source:** NHS England A&E Attendances and Emergency Admissions
- **Coverage:** April 2025 – February 2026 (11 months)
- **Granularity:** Provider level (NHS Trusts and Foundation Trusts)
- **Rows:** 2,197 across all months

## Pipeline

### Extract
Scrapes the NHS England statistics page and downloads all monthly A&E CSV files programmatically.

### Transform
PySpark job that loads all monthly CSVs into a single DataFrame, standardises column names to snake_case, removes null rows, and adds derived metrics including total attendances, total 4hr breaches, breach rate percentage, and total emergency admissions. Output saved as Parquet.

### Load
Loads processed Parquet data into Snowflake, creating the database and schema if they don't exist.

## dbt Models

Three-layer structure following dbt best practices:

```
staging/
  stg_nhs_ae_attendances       cleaned source data

intermediate/
  int_nhs_ae_by_trust          aggregated metrics per trust across all months

marts/
  mart_trust_performance       trust performance with CRITICAL/HIGH/MEDIUM/LOW bands
  mart_monthly_trends          national A&E trends by month
```

9 data quality tests covering null checks, uniqueness, and accepted value validation.

## Orchestration

Airflow DAG scheduled monthly on the 1st of each month at 06:00 UTC:

```
extract → transform → load_to_snowflake → dbt_run → dbt_test
```

## Project Structure

```
nhs-waitinglist-pipeline/
├── etl/
│   ├── extract/extract.py
│   ├── transform/transform.py
│   └── load/load_to_snowflake.py
├── dbt/
│   └── nhs_waiting_dbt/
│       └── models/
│           ├── staging/
│           ├── intermediate/
│           └── marts/
├── airflow/
│   └── dags/nhs_pipeline_dag.py
├── data/
│   ├── raw/
│   └── processed/
├── docker-compose.yml
└── requirements.txt
```

## Setup

### Prerequisites
- Python 3.11+
- Java 17 (for PySpark)
- Docker
- Snowflake account

### Install dependencies
```bash
pip3 install -r requirements.txt
```

### Environment variables
Create a `.env` file:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=NHS_WAITING
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Run the pipeline
```bash
python3 etl/extract/extract.py
python3 etl/transform/transform.py
python3 etl/load/load_to_snowflake.py
cd dbt/nhs_waiting_dbt && dbt run && dbt test
```

### Start Airflow
```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler -d
```

Visit http://localhost:8081 — login with admin/admin.

## Author
Daud Abdi · [github.com/Daudsaid](https://github.com/Daudsaid) · [daudabdi.com](https://daudabdi.com)