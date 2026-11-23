# Testing & Verification Strategy

## 1. Testing Philosophy

The Data Quality module is verified through an automated multi-layer test suite built with **pytest** and **PySpark local sessions**.

Testing principles enforced:
- **Zero Mocking of Core Logic**: Engine validation logic is executed using actual local Spark DataFrames.
- **Isolated Sandboxes**: Every test run uses isolated temporary folders (`tmp_path`) to prevent local disk pollution and race conditions.
- **Realistic Data Flaws**: Test fixtures mimic real-world Bronze data defects (malformed types, unhandled nulls, duplicates, future dates, and broken foreign keys).
- **Edge-Case Resilience**: Tests explicitly evaluate missing files, empty DataFrames, and multi-run isolation.

---

## 2. Test Suite Architecture

The test suite is organized into two primary files:

```
tests/
├── conftest.py                 # SparkSession fixture & Windows Hadoop environment configuration
├── test_quality_engine.py      # 10 unit tests verifying DataQualityEngine & Data Contracts
└── test_dq_pipeline.py         # 5 integration tests verifying DataQualityPipeline end-to-end
```

### 2.1. `tests/test_quality_engine.py` (Unit Tests)
1. `test_all_quality_rules`: Validates simultaneous execution of schema, null, duplicate, range, and referential integrity checks on synthetic data.
2. `test_row_count_failure`: Verifies that exceeding min/max row thresholds marks checks as `FAILED`.
3. `test_internal_column_name_conflicts`: Confirms that engine metadata columns (`_rejection_reason`, etc.) do not clash with user dataset columns.
4. `test_schema_failure`: Ensures missing schema columns trigger a `FAILED` status.
5. `test_empty_string_validation`: Tests `trim()`-based empty string rejection for mandatory string columns.
6. `test_open_ended_ranges`: Tests minimum-only (`val >= 0`), maximum-only, and exclusive bound conditions.
7. `test_date_range_and_future_dates`: Validates dynamic date boundaries and rejection of `order_date > current_date`.
8. `test_referential_integrity_different_keys`: Validates foreign key joins where foreign key name differs from reference key name (`order_id` vs `target_order_id`).
9. `test_contracts_definitions`: Confirms schema and contract definitions for all 5 target datasets.
10. `test_safe_cast_prevents_silent_loss`: Tests that malformed strings in numeric columns trigger quarantine reasons instead of Spark exceptions or silent null drops.

### 2.2. `tests/test_dq_pipeline.py` (Integration Tests)
1. `test_end_to_end_pipeline_execution`: Comprehensive pipeline execution across all 5 datasets using raw CSV and JSON fixtures. Verifies Silver Parquet counts, Quarantine Parquet counts, rejection reasons, and Quality Report schemas in JSON and Parquet.
2. `test_pipeline_idempotency`: Runs the pipeline twice against the exact same `run_id` and verifies that Silver Parquet record counts do not duplicate.
3. `test_multiple_run_id_isolation`: Executes two consecutive runs (`run_id="run1"` and `run_id="run2"`) and confirms partition folders coexist without data bleeding.
4. `test_missing_dataset_handling`: Simulates partial Bronze ingestion (e.g. only shipments present, postgres tables missing) and confirms graceful continuation.
5. `test_empty_dataframe_handling`: Tests pipeline execution when a Bronze input contains only headers and 0 data rows.

---

## 3. Running the Tests

To run the full suite:

```bash
pytest -v
```

### Environment Variables for Windows
```powershell
$env:HADOOP_HOME = "C:\Users\hp\hadoop"
$env:PATH = "C:\Users\hp\hadoop\bin;" + $env:PATH
$env:PYSPARK_PYTHON = "$pwd\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$pwd\.venv\Scripts\python.exe"

pytest -v
```

---

## 4. Verified Execution Results

```
============================= test session starts =============================
platform win32 -- Python 3.14.6, pytest-9.2.0, pluggy-1.7.0 -- C:\Users\hp\Data-Quality-Pipeline-Platform\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\hp\Data-Quality-Pipeline-Platform
configfile: pytest.ini
collecting ... collected 15 items

tests/test_dq_pipeline.py::test_end_to_end_pipeline_execution PASSED     [  6%]
tests/test_dq_pipeline.py::test_pipeline_idempotency PASSED              [ 13%]
tests/test_dq_pipeline.py::test_multiple_run_id_isolation PASSED         [ 20%]
tests/test_dq_pipeline.py::test_missing_dataset_handling PASSED          [ 26%]
tests/test_dq_pipeline.py::test_empty_dataframe_handling PASSED          [ 33%]
tests/test_quality_engine.py::test_all_quality_rules PASSED              [ 40%]
tests/test_quality_engine.py::test_row_count_failure PASSED              [ 46%]
tests/test_quality_engine.py::test_internal_column_name_conflicts PASSED [ 53%]
tests/test_quality_engine.py::test_schema_failure PASSED                 [ 60%]
tests/test_quality_engine.py::test_empty_string_validation PASSED        [ 66%]
tests/test_quality_engine.py::test_open_ended_ranges PASSED              [ 73%]
tests/test_quality_engine.py::test_date_range_and_future_dates PASSED    [ 80%]
tests/test_quality_engine.py::test_referential_integrity_different_keys PASSED [ 86%]
tests/test_quality_engine.py::test_contracts_definitions PASSED          [ 93%]
tests/test_quality_engine.py::test_safe_cast_prevents_silent_loss PASSED [100%]

======================= 15 passed in 561.57s (0:09:21) ========================
```
