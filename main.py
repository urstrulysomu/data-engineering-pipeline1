"""
main.py — Run the full ETL pipeline end to end.
This is the entry point. Run it with: python main.py
"""

from etl.extract import extract_from_api
from etl.transform import run_transforms
from etl.load import get_connection, create_table_from_records, load_records, query


def run_pipeline():
    print("=" * 50)
    print("  DATA ENGINEERING PIPELINE — Starting")
    print("=" * 50)

    # ── 1. EXTRACT ───────────────────────────────────
    # Sample data — replace with extract_from_api(), extract_from_csv(), etc.
    raw_data = [
        {"id": "1", "name": "Alice Johnson",  "email": "alice@example.com",  "city": "New York",    "company": "DataCorp"},
        {"id": "2", "name": "Bob Smith",      "email": "bob@example.com",    "city": "San Francisco","company": "PipelineInc"},
        {"id": "3", "name": "Carol White",    "email": "carol@example.com",  "city": "Austin",      "company": "StreamLabs"},
        {"id": "4", "name": "  Dave Brown ",  "email": " dave@example.com ", "city": "Chicago",     "company": "ETL Solutions"},
        {"id": "5", "name": "Eve Martinez",   "email": "",                   "city": "Seattle",     "company": "DataWave"},
        {"id": "6", "name": "Frank Lee",      "email": "frank@example.com",  "city": "Boston",      "company": "CloudPipe"},
    ]
    print(f"[EXTRACT] Loaded {len(raw_data)} sample records.")

    # ── 2. TRANSFORM ─────────────────────────────────
    schema = {}  # Add type casting here when needed, e.g. {"id": int}
    clean_data = run_transforms(
        raw_data,
        required_fields=["id", "name", "email"],
        schema=schema
    )

    # ── 3. LOAD ───────────────────────────────────────
    conn = get_connection("database/pipeline.db")
    create_table_from_records(conn, table="users", records=clean_data)
    load_records(conn, table="users", records=clean_data, mode="replace")

    # ── 4. VERIFY ─────────────────────────────────────
    print("\n--- Sample rows from database ---")
    results = query(conn, "SELECT id, name, email FROM users LIMIT 5")
    for row in results:
        print(f"  {row['id']:>3}  {row['name']:<25}  {row['email']}")

    count = query(conn, "SELECT COUNT(*) as total FROM users")[0]["total"]
    print(f"\n[DONE] Total records in database: {count}")
    print("=" * 50)
    conn.close()


if __name__ == "__main__":
    run_pipeline()
