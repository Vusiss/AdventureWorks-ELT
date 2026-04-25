from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess

from airflow.decorators import dag, task

PROJECT_DIR = Path(os.environ["PROJECT_DIR"])
DBT_DIR = PROJECT_DIR / "adventure_works_dbt"
VENV_PYTHON = str(PROJECT_DIR / ".venv_" / "bin" / "python")
VENV_DBT = str(PROJECT_DIR / ".venv_" / "bin" / "dbt")

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="adventure_works_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["adventureworks", "dbt", "etl"],
    doc_md="""
    Full ELT pipeline for AdventureWorks BI:
    1. Extract raw data (dlt from MSSQL + CSV + NBP API)
    2. Run dbt models (staging + marts)
    3. Run dbt tests (94 data quality checks)
    """,
)
def adventure_works_pipeline():

    @task()
    def extract_data():
        result = subprocess.run(
            [VENV_PYTHON, str(PROJECT_DIR / "data_extract.py")],
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
        return "extraction complete"

    @task()
    def dbt_run():
        result = subprocess.run(
            [
                VENV_DBT,
                "run",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(Path.home() / ".dbt"),
            ],
            cwd=str(DBT_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
        return "dbt run complete"

    @task()
    def dbt_test():
        result = subprocess.run(
            [
                VENV_DBT,
                "test",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(Path.home() / ".dbt"),
            ],
            cwd=str(DBT_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
        return "dbt test complete"

    extract = extract_data()
    run = dbt_run()
    test = dbt_test()

    extract >> run >> test


adventure_works_pipeline()
