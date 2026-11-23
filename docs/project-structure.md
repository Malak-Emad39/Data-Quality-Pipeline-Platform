# Project Structure & Repository Anatomy

## 1. Directory Tree

```
Data-Quality-Pipeline-Platform/
│
├── bronze/                             # Raw ingested datasets (Source of Truth)
│   ├── postgres/                       # PostgreSQL batch extracts
│   │   ├── customers/YYYY-MM-DD/       # customers_{run_id}.csv
│   │   ├── products/YYYY-MM-DD/        # products_{run_id}.csv
│   │   └── orders/YYYY-MM-DD/          # orders_{run_id}.csv
│   ├── csv_files/                      # Flat file ingestion
│   │   └── shipments/YYYY-MM-DD/       # shipments_{run_id}.csv
│   └── rest_api/                       # REST API extraction
│       └── products/YYYY-MM-DD/        # products_{run_id}.json
│
├── silver/                             # Validated, conformed Silver Parquet tables
│   ├── customers/run_id={id}/          # Clean customers Parquet files
│   ├── products/run_id={id}/           # Clean products Parquet files
│   ├── orders/run_id={id}/             # Clean orders Parquet files
│   ├── shipments/run_id={id}/          # Clean shipments Parquet files
│   └── api_products/run_id={id}/       # Clean API products Parquet files
│
├── rejected/                           # Quarantined non-compliant records
│   ├── customers/run_id={id}/          # Quarantined customer records
│   ├── products/run_id={id}/           # Quarantined product records
│   ├── orders/run_id={id}/             # Quarantined order records
│   ├── shipments/run_id={id}/          # Quarantined shipment records
│   └── api_products/run_id={id}/       # Quarantined API product records
│
├── quality_reports/                    # Dual-format audit reports
│   └── run_id={id}/                    # Partitioned execution metrics
│       ├── quality_report.json         # Human-readable JSON metrics
│       └── quality_report.parquet      # Columnar Parquet audit table
│
├── src/                                # Platform source code
│   ├── extraction/                     # Module 1: Ingestion & Extraction
│   │   ├── __init__.py                 # Extraction package marker
│   │   ├── ingestion.py                # Source extractor (Postgres, CSV, REST API)
│   │   └── sample_shipments.csv        # Local CSV template for shipments
│   │
│   └── validation/                     # Module 2: Data Quality & Contracts
│       ├── __init__.py                 # Validation package marker
│       ├── quality_engine.py           # Core PySpark DataQualityEngine
│       ├── contracts.py                # Declarative schemas and validation rules
│       └── dq_pipeline.py              # End-to-end pipeline runner & orchestrator
│
├── tests/                              # Automated test suite
│   ├── __init__.py                     # Tests package marker
│   ├── conftest.py                     # Spark fixtures and environment hooks
│   ├── test_quality_engine.py          # 10 unit tests for validation rules & casting
│   └── test_dq_pipeline.py             # 5 end-to-end and edge-case integration tests
│
├── docs/                               # Comprehensive project documentation
│   ├── architecture.md                 # Medallion lakehouse and system flow
│   ├── data-quality.md                 # Quality check suite and engine mechanics
│   ├── data-contracts.md               # Dataset contracts, schemas, and constraints
│   ├── pipeline-usage.md               # CLI manual, Python API, and Airflow guide
│   ├── testing.md                      # Testing strategy, suites, and pass results
│   └── project-structure.md            # Repository anatomy (this document)
│
├── .gitignore                          # Git ignore rules for virtual environments & artifacts
├── pytest.ini                          # Pytest configuration and pythonpath setup
├── requirements.txt                    # Project dependencies
└── README.md                           # Main repository entrypoint
```

---

## 2. Component Responsibilities

### `src/validation/quality_engine.py`
The low-level validation engine. Responsible for:
- Evaluating schema presence and safe type casting (`try_cast`).
- Executing null, empty-string, and duplicate detection.
- Performing referential integrity joins against parent Silver DataFrames.
- Evaluating open-ended and closed ranges and date constraints.
- Emitting partitioned clean DataFrames, quarantine DataFrames, and standardized 10-column quality report DataFrames.

### `src/validation/contracts.py`
The contract definition registry. Responsible for:
- Housing the declarative schemas and quality rules for all 5 business entities (`postgres.customers`, `postgres.products`, `postgres.orders`, `csv_files.shipments`, `rest_api.products`).
- Providing the `get_contract(name)` lookup factory.

### `src/validation/dq_pipeline.py`
The high-level pipeline orchestrator. Responsible for:
- Discovering Bronze files by date partition and `run_id`.
- Reading and exploding multi-line JSON API responses.
- Enforcing dataset execution order (`customers` → `products` → `orders` → `shipments`).
- Passing reference datasets to child contracts for referential integrity.
- Writing output Parquet partitions to Silver and Quarantine (`rejected/`).
- Writing audit reports in JSON and Parquet.

### `src/extraction/ingestion.py`
The data ingestion module (Member 1 responsibility). Extracts raw data from PostgreSQL, generates sample shipment records with injected data defects, fetches catalog items from the DummyJSON REST API, and stores them under `bronze/`.
