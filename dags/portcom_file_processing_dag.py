from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator


with DAG(dag_id="portcom_file_processing_dag", schedule=None) as dag:
    s3_hook = S3Hook(aws_conn_id="aws_s3")

    def list_keys():
        print(s3_hook.list_keys("portfolio-company-files"))

    list_s3_files = PythonOperator(
        task_id="list_s3_files",
        python_callable=list_keys,
    )

    hello_world = PythonOperator(
        task_id="hello_world",
        python_callable=lambda: print("Hello, world!"),
    )

    hello_world >> list_s3_files
