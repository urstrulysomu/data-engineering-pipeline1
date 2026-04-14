"""
extract.py — Pull raw data from various sources.
Currently supports: CSV files, JSON files, and REST APIs.
Add new sources here as your project grows.
"""

import csv
import json
import urllib.request
from pathlib import Path


def extract_from_csv(filepath: str) -> list[dict]:
    """Read a CSV file and return a list of row dictionaries."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]

    print(f"[EXTRACT] Loaded {len(data)} rows from {filepath}")
    return data


def extract_from_json(filepath: str) -> list[dict]:
    """Read a JSON file and return a list of records."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {filepath}")

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Support both a list at root level or a wrapped object
    if isinstance(data, dict):
        # Try common wrapper keys
        for key in ("data", "records", "results", "items"):
            if key in data:
                data = data[key]
                break

    print(f"[EXTRACT] Loaded {len(data)} records from {filepath}")
    return data


def extract_from_api(url: str, params: dict = None) -> list[dict]:
    """
    Fetch JSON data from a REST API endpoint.
    Example:
        records = extract_from_api("https://jsonplaceholder.typicode.com/users")
    """
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"

    with urllib.request.urlopen(url) as response:
        raw = response.read().decode("utf-8")
        data = json.loads(raw)

    if isinstance(data, dict):
        for key in ("data", "records", "results", "items"):
            if key in data:
                data = data[key]
                break

    print(f"[EXTRACT] Fetched {len(data)} records from API: {url}")
    return data
