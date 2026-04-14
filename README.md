# Data Engineering Pipeline

A modular ETL (Extract → Transform → Load) pipeline built with Python and SQL.

## Project Structure

```
data-engineering-pipeline1/
│
├── main.py                  # Entry point — run this!
│
├── etl/
│   ├── extract.py           # Pull data from CSV, JSON, or APIs
│   ├── transform.py         # Clean, normalize, and reshape data
│   └── load.py              # Write data to SQLite database
│
├── database/                # Auto-created — stores pipeline.db
├── data/
│   ├── raw/                 # Drop your raw CSV/JSON files here
│   └── processed/           # Cleaned output files (optional)
│
├── tests/                   # Unit tests (coming soon)
└── requirements.txt         # Dependencies
```

## How to Run

Make sure you have Python 3.10+ installed. No external packages needed!

```bash
python main.py
```

## How It Works

1. **Extract** — Pulls data from a source (API, CSV, JSON)
2. **Transform** — Cleans column names, drops nulls, casts types, adds timestamps
3. **Load** — Inserts clean data into a local SQLite database
4. **Verify** — Prints a sample of what was loaded

## Future Enhancements

- [ ] Connect to a real database (PostgreSQL, BigQuery)
- [ ] Add scheduling with Apache Airflow
- [ ] Add data validation with Great Expectations
- [ ] Build a real-time streaming pipeline with Kafka
- [ ] Add CI/CD with GitHub Actions
