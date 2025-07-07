from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from operators.minio_to_clickhouse.MinIOToClickHouseOperator import MinIOToClickHouseOperator
 # Replace with actual path

MINIO_CONN_ID = "minio_conn"
CLICKHOUSE_CONN_ID = "clickhouse_conn"
BUCKET = "search-analytics"
PREFIX = ""
DATABASE = "default"

with DAG(
    dag_id="load_raw_json_files",
    start_date=datetime(2025, 6, 1),
    schedule='@hourly',
    catchup=False,
    tags=["minio", "clickhouse", "json"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Hook to list files in MinIO bucket
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    keys = s3.list_keys(bucket_name=BUCKET, prefix=PREFIX) or []

    json_keys = [key for key in keys if key.endswith(".json")]

    for key in json_keys:
        task_id = f"load_{key.split('/')[-1].replace('.json', '')}"

        load_task = MinIOToClickHouseOperator(
            task_id=task_id,
            minio_conn_id=MINIO_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            bucket=BUCKET,
            key=key,
            database=DATABASE,
              # Optional: if your operator supports this
        )

        start >> load_task >> end
