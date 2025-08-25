from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.sdk import dag, task


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
    dag_id="portcom_file_processing_dag",
    schedule=None,
    catchup=False,
)
def portcom_file_processing_dag():
    # Define the S3ListOperator within the DAG context
    list_s3_keys = S3ListOperator(
        task_id="list_s3_files",
        bucket="portfolio-company-files",
        aws_conn_id="aws_s3",
    )

    # Process the files
    file_keys = log_and_return_keys(list_s3_keys.output)
    parse_file.expand(file_key=file_keys)


# Instantiate the DAG
portcom_file_processing_dag()
