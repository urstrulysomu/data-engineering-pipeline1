"""
transform.py — Clean and reshape raw data before loading.
Each function takes a list of dicts and returns a cleaned list of dicts.
Add your own transform functions here as business logic grows.
"""

from datetime import datetime


def clean_strings(records: list[dict]) -> list[dict]:
    """Strip whitespace from all string values."""
    cleaned = []
    for row in records:
        cleaned.append({k: v.strip() if isinstance(v, str) else v for k, v in row.items()})
    return cleaned


def drop_nulls(records: list[dict], required_fields: list[str]) -> list[dict]:
    """Remove rows that are missing values in required fields."""
    before = len(records)
    filtered = [
        row for row in records
        if all(row.get(field) not in (None, "", "NULL", "null") for field in required_fields)
    ]
    dropped = before - len(filtered)
    if dropped:
        print(f"[TRANSFORM] Dropped {dropped} rows with missing required fields: {required_fields}")
    return filtered


def normalize_column_names(records: list[dict]) -> list[dict]:
    """Lowercase all column names and replace spaces with underscores."""
    if not records:
        return records
    return [
        {k.lower().replace(" ", "_"): v for k, v in row.items()}
        for row in records
    ]


def add_timestamp(records: list[dict], column: str = "loaded_at") -> list[dict]:
    """Add a timestamp column to every record."""
    now = datetime.utcnow().isoformat()
    return [{**row, column: now} for row in records]


def cast_types(records: list[dict], schema: dict) -> list[dict]:
    """
    Cast column values to specified Python types.

    Example schema:
        schema = {
            "age": int,
            "salary": float,
            "is_active": bool
        }
    """
    result = []
    for row in records:
        new_row = dict(row)
        for col, cast_fn in schema.items():
            if col in new_row and new_row[col] not in (None, ""):
                try:
                    new_row[col] = cast_fn(new_row[col])
                except (ValueError, TypeError) as e:
                    print(f"[TRANSFORM] Warning: could not cast '{col}' value '{new_row[col]}' → {e}")
        result.append(new_row)
    return result


def run_transforms(records: list[dict], required_fields: list[str] = None, schema: dict = None) -> list[dict]:
    """
    Run the full default transformation pipeline in order:
    1. Normalize column names
    2. Clean string whitespace
    3. Drop rows missing required fields
    4. Cast types (if schema provided)
    5. Add load timestamp
    """
    records = normalize_column_names(records)
    records = clean_strings(records)
    if required_fields:
        records = drop_nulls(records, required_fields)
    if schema:
        records = cast_types(records, schema)
    records = add_timestamp(records)
    print(f"[TRANSFORM] Pipeline complete. {len(records)} records ready to load.")
    return records
