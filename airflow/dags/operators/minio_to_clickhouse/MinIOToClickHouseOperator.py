from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import pandas as pd
import json
import re
from io import BytesIO


class MinIOToClickHouseOperator(BaseOperator):
    def __init__(
        self,
        minio_conn_id: str,
        clickhouse_conn_id: str,
        bucket: str,
        key: str,
        database: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.minio_conn_id = minio_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id
        self.bucket = bucket
        self.key = key
        self.database = database

    def clean_datetime_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dropna().astype(str).str.contains(r'\bUTC\b', na=False).any():
                df[col] = df[col].str.replace(r'\s*UTC\s*', '', regex=True).str.strip()
                self.log.info(f"Removed 'UTC' from datetime column: {col}")
        return df

    def auto_detect_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?',
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',
            r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
        ]

        for col in df.select_dtypes(include=['object']).columns:
            if df[col].isna().all():
                continue

            sample_values = df[col].dropna().head(100).astype(str)
            if len(sample_values) == 0:
                continue

            datetime_matches = 0
            for pattern in datetime_patterns:
                matches = sample_values.str.match(pattern, na=False).sum()
                datetime_matches = max(datetime_matches, matches)

            if datetime_matches > len(sample_values) * 0.8:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                    df[col] = df[col].dt.tz_convert(None)  # Remove timezone awareness
                    self.log.info(f"Auto-converted '{col}' to datetime")
                except Exception as e:
                    self.log.warning(f"Failed to convert '{col}' to datetime: {e}")
                    continue

        return df

    def infer_clickhouse_schema(self, df: pd.DataFrame):
        type_map = {
            'int64': 'Nullable(Int64)',
            'float64': 'Nullable(Float64)',
            'bool': 'Nullable(UInt8)',
            'object': 'Nullable(String)',
            'datetime64[ns]': 'Nullable(DateTime64(3))',
            'list': 'Nullable(String)', 
        }

        schema = {}
        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample_value, list):
                schema[col] = 'Nullable(String)'
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
                continue
            if 'datetime64' in dtype_str:
                schema[col] = 'Nullable(DateTime64(3))'
            else:
                schema[col] = type_map.get(dtype_str, 'Nullable(String)')
        return schema

    def execute(self, context):
        # Step 1: Read JSON file from MinIO
        s3 = S3Hook(aws_conn_id=self.minio_conn_id)
        self.log.info(f"Loading JSON file from MinIO: {self.key}")
        obj = s3.get_key(bucket_name=self.bucket, key=self.key)
        content = obj.get()['Body'].read().decode('utf-8').strip()

        # Step 2: Parse JSON safely (handle single object OR multi-line JSON objects)
        try:
            json_data = json.loads(content)
            if isinstance(json_data, dict):
                json_data = [json_data]
        except json.JSONDecodeError:
            try:
                json_data = [json.loads(line) for line in content.splitlines() if line.strip()]
            except Exception as e:
                self.log.error(f"Failed to parse JSON lines: {e}")
                raise

        if not json_data:
            self.log.warning(f"No valid JSON data found in {self.key}. Skipping.")
            return

        # Step 3: Normalize to DataFrame
        df = pd.json_normalize(json_data)

        if df.empty:
            self.log.warning(f"No data found in {self.key}. Skipping.")
            return

        self.log.info(f"Read {len(df)} rows and {len(df.columns)} columns from JSON.")

        # Step 3.5: Clean datetime strings (remove 'UTC' suffixes)
        df = self.clean_datetime_strings(df)

        # Step 4: Auto-detect and convert datetime columns
        df = self.auto_detect_datetime_columns(df)

        # Step 5: Build table name and infer schema
        table_name = self.key.split('/')[-1].replace('.json', '').lower()
        full_table = f"{self.database}.{table_name}"

        schema = self.infer_clickhouse_schema(df)
        columns_sql = ", ".join(f"{col} {col_type}" for col, col_type in schema.items())

        df = df.astype(object).where(pd.notnull(df), None)

        # Step 6: Create table in ClickHouse
        ch = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        conn = ch.get_conn()

        self.log.info(f"Dropping existing table {full_table} (if any)...")
        conn.execute(f"DROP TABLE IF EXISTS {full_table}")

        self.log.info(f"Creating table {full_table} with schema: {schema}")
        conn.execute(f"""
            CREATE TABLE {full_table} (
                {columns_sql}
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
        """)

        # Step 7: Insert Data
        columns = list(df.columns)
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        insert_query = f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES"
        conn.execute(insert_query, data)

        self.log.info(f"Inserted {len(df)} rows into {full_table}")
