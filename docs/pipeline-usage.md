# Pipeline Usage & Operations Guide

## 1. Overview

The `DataQualityPipeline` (`src/validation/dq_pipeline.py`) can be executed as a standalone Python CLI script, called as a library within ETL applications, or orchestrated as an Apache Airflow DAG task.

---

## 2. Command Line Execution

### Direct Execution
To execute the pipeline against the latest Bronze ingestion or a specific run identifier:

```bash
# Execute against the most recent Bronze run_id automatically
python -m src.validation.dq_pipeline

# Execute against a specific run_id
python -m src.validation.dq_pipeline --run-id 8f488fe2

# Custom directory configuration
python -m src.validation.dq_pipeline \
    --run-id 8f488fe2 \
    --bronze-dir bronze \
    --silver-dir silver \
    --rejected-dir rejected \
    --reports-dir quality_reports
```

### Windows Environment Preparation
When running on Windows with Apache Spark, ensure `HADOOP_HOME` and Python worker paths are set:

```powershell
$env:HADOOP_HOME = "C:\Users\hp\hadoop"
$env:PATH = "C:\Users\hp\hadoop\bin;" + $env:PATH
$env:PYSPARK_PYTHON = "C:\Users\hp\Data-Quality-Pipeline-Platform\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\hp\Data-Quality-Pipeline-Platform\.venv\Scripts\python.exe"

python -m src.validation.dq_pipeline
```

---

## 3. Programmatic Usage in Python

```python
from pyspark.sql import SparkSession
from src.validation.dq_pipeline import DataQualityPipeline

# 1. Initialize SparkSession
spark = (
    SparkSession.builder
    .appName("Production-DataQualityPipeline")
    .master("local[*]")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

# 2. Instantiate Pipeline
pipeline = DataQualityPipeline(
    spark=spark,
    bronze_dir="bronze",
    silver_dir="silver",
    rejected_dir="rejected",
    reports_dir="quality_reports",
)

# 3. Execute Pipeline
run_summary = pipeline.run_pipeline(run_id="8f488fe2")

# 4. Inspect Results
print("Pipeline Run Status:", run_summary["status"])
for dataset_name, stats in run_summary["datasets"].items():
    print(f"Dataset {dataset_name}: {stats['records_valid']} valid, {stats['records_rejected']} quarantined.")
```

---

## 4. Airflow Orchestration Pattern

Below is an example Apache Airflow PythonOperator task definition integrating the Data Quality pipeline:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_data_quality_task(**context):
    from pyspark.sql import SparkSession
    from src.validation.dq_pipeline import DataQualityPipeline

    ti = context["ti"]
    # Retrieve run_id pushed by Member 1 (Ingestion Task)
    run_id = ti.xcom_pull(task_ids="ingest_bronze_task", key="run_id")

    spark = SparkSession.builder.appName("Airflow-DQ").getOrCreate()
    pipeline = DataQualityPipeline(spark=spark)
    result = pipeline.run_pipeline(run_id=run_id)

    if result["status"] != "SUCCESS":
        raise ValueError(f"Data Quality Pipeline failed for run {run_id}")

    # Push valid datasets summary to downstream Gold aggregation tasks
    ti.xcom_push(key="dq_summary", value=result["datasets"])

default_args = {
    "owner": "data_engineers",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "depi_medallion_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    dq_task = PythonOperator(
        task_id="data_quality_validation",
        python_callable=run_data_quality_task,
        provide_context=True,
    )
```

---

## 5. Output Verification & Inspection

### Querying Silver Tables
```python
silver_orders = spark.read.parquet("silver/orders/run_id=8f488fe2")
silver_orders.show(10, truncate=False)
```

### Inspecting Quarantined Rows & Reasons
```python
quarantine_shipments = spark.read.parquet("rejected/shipments/run_id=8f488fe2")
quarantine_shipments.select("shipment_id", "carrier", "rejection_reason", "rejection_timestamp").show(truncate=False)
```

### Inspecting Quality Reports
```python
reports = spark.read.parquet("quality_reports/run_id=8f488fe2/quality_report.parquet")
reports.filter(reports.check_status == "FAILED").show(truncate=False)
```
