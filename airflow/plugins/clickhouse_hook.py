from airflow.hooks.base import BaseHook
import clickhouse_connect

class ClickHouseHook(BaseHook):
    """
    Custom ClickHouse Hook using clickhouse-connect
    """

    def __init__(self, clickhouse_conn_id: str = 'clickhouse_conn'):
        super().__init__()
        self.conn_id = clickhouse_conn_id
        self.conn = self.get_connection(self.conn_id)

        self.host = self.conn.host
        self.port = self.conn.port or 8123
        self.username = self.conn.login
        self.password = self.conn.password
        self.database = self.conn.schema or 'default'

        self.client = None

    def get_client(self):
        if not self.client:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
        return self.client

    def execute(self, sql: str):
        client = self.get_client()
        client.command(sql)
