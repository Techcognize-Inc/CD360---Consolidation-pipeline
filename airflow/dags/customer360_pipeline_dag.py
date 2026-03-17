from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Paths for WSL/Linux
PROJECT_ROOT = "/mnt/c/Customer360"
VENV_PYTHON = f"{PROJECT_ROOT}/airflow_venv/bin/python"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="customer360_pipeline",
    description="Customer360 pipeline: stream -> consolidate -> segment",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["customer360", "spark", "delta", "openlineage"],
) as dag:

    consolidate_customer360 = BashOperator(
        task_id="consolidate_customer360",
        bash_command=(
            f'cd {PROJECT_ROOT} && '
            f'{VENV_PYTHON} src/customer360_consolidation.py'
        ),
    )

    segment_customers = BashOperator(
        task_id="segment_customers",
        bash_command=(
            f'cd {PROJECT_ROOT} && '
            f'{VENV_PYTHON} src/segmentation.py'
        ),
    )

    consolidate_customer360 >> segment_customers