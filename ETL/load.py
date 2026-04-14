"""
load.py — Write transformed records into a SQLite database.
SQLite requires zero setup — the database file is created automatically.
Swap out for PostgreSQL or BigQuery later by changing only this file.
"""

import sqlite3
from pathlib import Path


def get_connection(db_path: str = "database/pipeline.db") -> sqlite3.Connection:
    """Open (or create) a SQLite database and return the connection."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Rows behave like dicts
    print(f"[LOAD] Connected to database: {db_path}")
    return conn


def create_table_from_records(conn: sqlite3.Connection, table: str, records: list[dict]):
    """
    Auto-create a table based on the keys in the first record.
    All columns are stored as TEXT — cast in SQL queries as needed.
    Skips creation if the table already exists.
    """
    if not records:
        print("[LOAD] No records — skipping table creation.")
        return

    columns = ", ".join(f'"{col}" TEXT' for col in records[0].keys())
    sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})'
    conn.execute(sql)
    conn.commit()
    print(f"[LOAD] Table '{table}' ready.")


def load_records(conn: sqlite3.Connection, table: str, records: list[dict], mode: str = "append"):
    """
    Insert records into a table.

    mode options:
        "append"  — add rows without deleting existing ones (default)
        "replace" — clear the table first, then insert fresh
    """
    if not records:
        print("[LOAD] Nothing to load.")
        return

    if mode == "replace":
        conn.execute(f'DELETE FROM "{table}"')
        print(f"[LOAD] Cleared existing rows from '{table}'.")

    columns = list(records[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

    rows = [tuple(row[c] for c in columns) for row in records]
    conn.executemany(sql, rows)
    conn.commit()
    print(f"[LOAD] Inserted {len(rows)} rows into '{table}'.")


def query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    """Run a SQL SELECT and return results as a list of dicts."""
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]
