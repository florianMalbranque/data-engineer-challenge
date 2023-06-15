import sqlite3
from typing import List, Tuple


class SQLiteClient:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()

    def close(self):
        if self.connection:
            self.connection.commit()
            self.cursor.close()
            self.connection.close()

    def create_table(self, table_name: str, column_defs: str):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
        self.cursor.execute(query)

    def batch_insert(self, table_name: str, columns: List[str], values: List[Tuple]):
        placeholders = ", ".join(["?"] * len(columns))
        query = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        )
        self.cursor.executemany(query, values)
        self.connection.commit()

    def fetch_all(self, table_name: str) -> List[Tuple]:
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        return self.cursor.fetchall()
