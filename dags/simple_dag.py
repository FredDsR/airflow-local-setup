from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG("simple_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
    run_script = BashOperator(
        task_id="run_script", bash_command="echo 'Hello from script'"
    )

    empty = EmptyOperator(task_id="empty_task")

    empty >> run_script
