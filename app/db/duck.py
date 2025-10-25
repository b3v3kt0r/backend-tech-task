from pathlib import Path
from typing import Any, List

import duckdb


BASE_DIR = Path(__file__).resolve().parents[2]
DUCKDB_PATH = BASE_DIR / "analytics.duckdb"


def get_duck_conn(read_only: bool = False):
    """
    Establish a connection to the DuckDB database.
    """

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def query_analytics(sql: str, params: List[Any] = None):
    """
    Execute a read-only SQL query on the DuckDB database and return results.
    """

    conn = get_duck_conn(read_only=True)

    try:
        if params:
            return conn.execute(sql, params).fetchall()
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def write_analytics(conn, sql: str, data: list = None):
    """
    Execute a write operation (INSERT, UPDATE, DELETE) on the DuckDB database.
    """

    if data:
        conn.executemany(sql, data)
    else:
        conn.execute(sql)
    conn.commit()
