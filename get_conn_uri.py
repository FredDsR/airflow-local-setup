from airflow.sdk import Connection

c = Connection(
    conn_id="aws_s3",
    conn_type="aws",
    login="<AWS_ACCESS_KEY_ID>",
    password="<AWS_SECRET_ACCESS_KEY>",
)

print(c.get_uri())
