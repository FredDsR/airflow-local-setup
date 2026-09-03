"""List objects in an S3 bucket and fan out one task per key.

Demonstrates wiring a classic operator (`S3ListOperator`) into TaskFlow tasks
and using dynamic task mapping over its XCom output.
"""

from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.sdk import dag, task

S3_BUCKET = "airflow-demo-files"
AWS_CONN_ID = "aws_s3"


@task
def log_and_return_keys(file_keys: list[str]):
    """Log the S3 file keys and return them.

    Args:
        file_keys (list[str]): The list of S3 file keys.

    Returns:
        list[str]: The list of S3 file keys.
    """
    print(f"Processing {len(file_keys)} files: {file_keys}")
    return file_keys


@task
def parse_file(file_key: str):
    print(f"Parsing file: {file_key}")


@dag(
    dag_id="s3_file_processing_dag",
    schedule=None,
    catchup=False,
)
def s3_file_processing_dag():
    # Define the S3ListOperator within the DAG context
    list_s3_keys = S3ListOperator(
        task_id="list_s3_files",
        bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
    )

    # Process the files
    file_keys = log_and_return_keys(list_s3_keys.output)
    parse_file.expand(file_key=file_keys)


# Instantiate the DAG
s3_file_processing_dag()
