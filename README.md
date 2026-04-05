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