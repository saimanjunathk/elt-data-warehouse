# ELT Data Warehouse

End-to-end ELT pipeline with dbt, Airflow, BigQuery/Snowflake and data quality checks.

## Architecture
REST APIs / CSVs / DB -> Ingestion -> BigQuery/Snowflake -> dbt Transformations -> Metabase/Looker

## Tech Stack
- Ingestion: Python, REST APIs
- Warehouse: BigQuery / Snowflake
- Transformation: dbt
- Orchestration: Apache Airflow / Prefect
- Data Quality: Great Expectations
- Dashboard: Metabase / Looker

## Status
In Progress
