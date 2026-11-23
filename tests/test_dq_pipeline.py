"""
Integration tests for the Data Quality Pipeline (Phase 5).

Verifies the end-to-end integration between Bronze data sources,
DataQualityEngine, Data Contracts, Silver Parquet storage, Quarantine Parquet storage,
and JSON/Parquet Quality Reports.
"""

import json
import os
import shutil
import pytest
from pyspark.sql import SparkSession

from src.validation.dq_pipeline import DataQualityPipeline


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("DQPipelineIntegrationTests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def test_environment(tmp_path):
    """Sets up a complete realistic Bronze environment with intentional data quality flaws."""
    bronze_dir = str(tmp_path / "bronze")
    silver_dir = str(tmp_path / "silver")
    rejected_dir = str(tmp_path / "rejected")
    reports_dir = str(tmp_path / "quality_reports")

    day = "2026-11-22"
    run_id = "testrun1"

    # 1. postgres.customers
    cust_dir = os.path.join(bronze_dir, "postgres", "customers", day)
    os.makedirs(cust_dir, exist_ok=True)
    cust_file = os.path.join(cust_dir, f"customers_{run_id}.csv")
    with open(cust_file, "w", encoding="utf-8") as f:
        f.write("customer_id,name,email,country,updated_at\n")
        f.write("1,Ahmed,ahmed@example.com,Egypt,2026-11-20 10:00:00\n")          # Valid
        f.write("2,Mona,,Egypt,2026-11-20 10:00:00\n")                           # NULL email -> Rejected
        f.write("3,   ,sara@example.com,Egypt,2026-11-20 10:00:00\n")             # Empty whitespace name -> Rejected
        f.write("4,Omar,omar@example.com,Egypt,2026-11-20 10:00:00\n")            # Duplicate ID 4 -> Rejected
        f.write("4,Omar Duplicate,omar2@example.com,Egypt,2026-11-20 10:00:00\n") # Duplicate ID 4 -> Rejected
        f.write("5,Ali,ali@example.com,Egypt,2026-11-20 10:00:00\n")              # Valid

    # 2. postgres.products
    prod_dir = os.path.join(bronze_dir, "postgres", "products", day)
    os.makedirs(prod_dir, exist_ok=True)
    prod_file = os.path.join(prod_dir, f"products_{run_id}.csv")
    with open(prod_file, "w", encoding="utf-8") as f:
        f.write("product_id,name,price,updated_at\n")
        f.write("10,Laptop,1200.50,2026-11-20 10:00:00\n")       # Valid
        f.write("20,Mouse,-15.00,2026-11-20 10:00:00\n")         # Negative price -> Rejected
        f.write("30,Keyboard,corrupt_val,2026-11-20 10:00:00\n")  # Corrupt price -> Rejected
        f.write("40,Monitor,350.00,2026-11-20 10:00:00\n")       # Valid

    # 3. postgres.orders
    ord_dir = os.path.join(bronze_dir, "postgres", "orders", day)
    os.makedirs(ord_dir, exist_ok=True)
    ord_file = os.path.join(ord_dir, f"orders_{run_id}.csv")
    with open(ord_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,amount,order_date,updated_at\n")
        f.write("1001,1,10,2,2401.00,2026-11-10,2026-11-20 10:00:00\n")  # Valid (cust 1, prod 10)
        f.write("1002,9999,10,1,1200.50,2026-11-10,2026-11-20 10:00:00\n") # Orphan customer -> Rejected (RI)
        f.write("1003,1,8888,1,100.00,2026-11-10,2026-11-20 10:00:00\n")   # Orphan product -> Rejected (RI)
        f.write("1004,1,10,0,1200.50,2026-11-10,2026-11-20 10:00:00\n")    # Zero quantity -> Rejected (Range)
        f.write("1005,1,10,1,-50.00,2026-11-10,2026-11-20 10:00:00\n")     # Negative amount -> Rejected (Range)
        f.write("1006,1,10,1,1200.50,2099-01-01,2026-11-20 10:00:00\n")    # Future date -> Rejected (Date Range)
        f.write("1007,5,40,1,350.00,2026-11-10,2026-11-20 10:00:00\n")     # Valid (cust 5, prod 40)
        f.write("1008,1,10,1,100.00,2026-11-10,2026-11-20 10:00:00\n")     # Duplicate ID 1008 -> Rejected
        f.write("1008,1,10,1,100.00,2026-11-10,2026-11-20 10:00:00\n")     # Duplicate ID 1008 -> Rejected

    # 4. csv_files.shipments
    ship_dir = os.path.join(bronze_dir, "csv_files", "shipments", day)
    os.makedirs(ship_dir, exist_ok=True)
    ship_file = os.path.join(ship_dir, f"shipments_{run_id}.csv")
    with open(ship_file, "w", encoding="utf-8") as f:
        f.write("shipment_id,order_id,carrier,shipped_date,delivery_days\n")
        f.write("501,1001,DHL,2026-11-11,2\n")     # Valid (order 1001 is valid)
        f.write("502,5000,FedEx,2026-11-11,3\n")   # Orphan order_id 5000 -> Rejected (RI)
        f.write("503,1001,,2026-11-11,2\n")        # Empty carrier -> Rejected (Empty/NULL)
        f.write("504,1001,Aramex,2026-11-11,-1\n") # Negative delivery_days -> Rejected (Range)
        f.write("505,1001,DHL,2026-11-11,2\n")     # Duplicate ID 505 -> Rejected
        f.write("505,1001,DHL,2026-11-11,2\n")     # Duplicate ID 505 -> Rejected

    # 5. rest_api.products (DummyJSON format)
    api_dir = os.path.join(bronze_dir, "rest_api", "products", day)
    os.makedirs(api_dir, exist_ok=True)
    api_file = os.path.join(api_dir, f"products_{run_id}.json")
    api_pages = [
        {
            "products": [
                {"id": 201, "title": "Headphones", "category": "electronics", "price": 99.99, "rating": 4.8, "stock": 50},  # Valid
                {"id": 202, "title": "Tablet", "category": "electronics", "price": 299.99, "rating": 5.5, "stock": 10},      # Rating > 5 -> Rejected
                {"id": 203, "title": "Cable", "category": "electronics", "price": 9.99, "rating": 4.2, "stock": -3},         # Stock < 0 -> Rejected
                {"id": 204, "title": "Camera", "category": "electronics", "price": 450.00, "rating": 4.6, "stock": 5},       # Valid
            ],
            "total": 4,
            "skip": 0,
            "limit": 30,
        }
    ]
    with open(api_file, "w", encoding="utf-8") as f:
        json.dump(api_pages, f)

    return {
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "rejected_dir": rejected_dir,
        "reports_dir": reports_dir,
        "run_id": run_id,
        "day": day,
    }


def test_end_to_end_pipeline_execution(spark, test_environment):
    """Tests complete end-to-end execution of the Data Quality Pipeline."""
    env = test_environment
    pipeline = DataQualityPipeline(
        spark=spark,
        bronze_dir=env["bronze_dir"],
        silver_dir=env["silver_dir"],
        rejected_dir=env["rejected_dir"],
        reports_dir=env["reports_dir"],
    )

    results = pipeline.run_pipeline(run_id=env["run_id"])

    assert results["status"] == "SUCCESS"
    assert results["run_id"] == env["run_id"]
    assert results["duration_seconds"] > 0

    # 1. Verify Customers validation results
    cust_stats = results["datasets"]["postgres.customers"]
    assert cust_stats["records_checked"] == 6
    assert cust_stats["records_valid"] == 2      # IDs 1, 5
    assert cust_stats["records_rejected"] == 4   # Mona (null), Sara (empty), Omar (duplicates)

    # 2. Verify Products validation results
    prod_stats = results["datasets"]["postgres.products"]
    assert prod_stats["records_checked"] == 4
    assert prod_stats["records_valid"] == 2      # Laptops (10) and Monitors (40)
    assert prod_stats["records_rejected"] == 2   # Negative price and corrupt string price

    # 3. Verify Orders validation results
    ord_stats = results["datasets"]["postgres.orders"]
    assert ord_stats["records_checked"] == 9
    assert ord_stats["records_valid"] == 2       # Orders 1001 and 1007
    assert ord_stats["records_rejected"] == 7

    # 4. Verify Shipments validation results
    ship_stats = results["datasets"]["csv_files.shipments"]
    assert ship_stats["records_checked"] == 6
    assert ship_stats["records_valid"] == 1      # Shipment 501
    assert ship_stats["records_rejected"] == 5

    # 5. Verify API Products validation results
    api_stats = results["datasets"]["rest_api.products"]
    assert api_stats["records_checked"] == 4
    assert api_stats["records_valid"] == 2       # Headphones and Camera
    assert api_stats["records_rejected"] == 2    # Rating > 5 and Stock < 0

    # 6. Verify Silver Parquet outputs
    customers_silver = spark.read.parquet(
        os.path.join(env["silver_dir"], "customers", f"run_id={env['run_id']}")
    )
    assert customers_silver.count() == 2
    assert "email" in customers_silver.columns

    # 7. Verify Rejected Quarantine Parquet outputs
    customers_rejected = spark.read.parquet(
        os.path.join(env["rejected_dir"], "customers", f"run_id={env['run_id']}")
    )
    assert customers_rejected.count() == 4
    assert "run_id" in customers_rejected.columns
    assert "rejection_reason" in customers_rejected.columns
    assert "rejection_timestamp" in customers_rejected.columns

    reasons = [row["rejection_reason"] for row in customers_rejected.collect()]
    assert any("email cannot be NULL" in r for r in reasons)
    assert any("name cannot be empty" in r for r in reasons)
    assert any("Duplicate record" in r for r in reasons)

    # 8. Verify Orders Quarantine reasons (RI, range, date range)
    orders_rejected = spark.read.parquet(
        os.path.join(env["rejected_dir"], "orders", f"run_id={env['run_id']}")
    )
    order_reasons = [row["rejection_reason"] for row in orders_rejected.collect()]
    assert any("has no matching reference" in r for r in order_reasons)
    assert any("quantity must be > 0" in r for r in order_reasons)
    assert any("amount must be > 0" in r for r in order_reasons)
    assert any("order_date cannot be in the future" in r for r in order_reasons)

    # 9. Verify Quality Reports completeness in JSON and Parquet
    assert os.path.exists(results["report_parquet"])
    assert os.path.exists(results["report_json"])

    with open(results["report_json"], "r", encoding="utf-8") as f:
        report_records = json.load(f)

    assert len(report_records) > 0
    sample_report = report_records[0]
    expected_fields = [
        "run_id",
        "pipeline_name",
        "task_name",
        "check_name",
        "check_status",
        "records_checked",
        "records_failed",
        "threshold",
        "execution_timestamp",
        "error_message",
    ]
    for field in expected_fields:
        assert field in sample_report


def test_pipeline_idempotency(spark, test_environment):
    """Tests that rerunning the pipeline with the same run_id overwrites cleanly without duplication."""
    env = test_environment
    pipeline = DataQualityPipeline(
        spark=spark,
        bronze_dir=env["bronze_dir"],
        silver_dir=env["silver_dir"],
        rejected_dir=env["rejected_dir"],
        reports_dir=env["reports_dir"],
    )

    # First run
    res1 = pipeline.run_pipeline(run_id=env["run_id"])
    silver_path = os.path.join(
        env["silver_dir"], "customers", f"run_id={env['run_id']}"
    )
    count1 = spark.read.parquet(silver_path).count()

    # Second run with same run_id
    res2 = pipeline.run_pipeline(run_id=env["run_id"])
    count2 = spark.read.parquet(silver_path).count()

    # Count must remain exactly the same, not doubled
    assert count1 == count2 == 2


def test_multiple_run_id_isolation(spark, test_environment):
    """Tests that separate runs produce isolated partition folders without overwriting each other."""
    env = test_environment
    pipeline = DataQualityPipeline(
        spark=spark,
        bronze_dir=env["bronze_dir"],
        silver_dir=env["silver_dir"],
        rejected_dir=env["rejected_dir"],
        reports_dir=env["reports_dir"],
    )

    # Create run 2 files
    run2_id = "testrun2"
    cust_dir = os.path.join(env["bronze_dir"], "postgres", "customers", env["day"])
    cust2_file = os.path.join(cust_dir, f"customers_{run2_id}.csv")
    with open(cust2_file, "w", encoding="utf-8") as f:
        f.write("customer_id,name,email,country,updated_at\n")
        f.write("100,Kareem,kareem@example.com,Egypt,2026-11-20 10:00:00\n")

    # Run for run1
    pipeline.run_pipeline(run_id=env["run_id"])
    # Run for run2
    pipeline.run_pipeline(run_id=run2_id)

    # Both run folders must coexist in Silver
    run1_path = os.path.join(
        env["silver_dir"], "customers", f"run_id={env['run_id']}"
    )
    run2_path = os.path.join(
        env["silver_dir"], "customers", f"run_id={run2_id}"
    )

    assert os.path.exists(run1_path)
    assert os.path.exists(run2_path)

    run1_count = spark.read.parquet(run1_path).count()
    run2_count = spark.read.parquet(run2_path).count()

    assert run1_count == 2
    assert run2_count == 1


def test_missing_dataset_handling(spark, tmp_path):
    """Tests that pipeline gracefully handles runs where some bronze files are missing."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    rejected_dir = tmp_path / "rejected"
    reports_dir = tmp_path / "reports"

    # Only create shipments, no postgres or api datasets
    ship_dir = bronze_dir / "csv_files" / "shipments" / "2026-11-23"
    ship_dir.mkdir(parents=True)
    ship_file = ship_dir / "shipments_missrun.csv"
    ship_file.write_text(
        "shipment_id,order_id,carrier,shipped_date,delivery_days\n"
        "1,101,FedEx,2026-11-20,3\n",
        encoding="utf-8"
    )

    pipeline = DataQualityPipeline(
        spark=spark,
        bronze_dir=str(bronze_dir),
        silver_dir=str(silver_dir),
        rejected_dir=str(rejected_dir),
        reports_dir=str(reports_dir),
    )

    res = pipeline.run_pipeline(run_id="missrun")
    assert res["status"] == "SUCCESS"
    assert "csv_files.shipments" in res["datasets"]
    assert "postgres.customers" not in res["datasets"]
    assert res["datasets"]["csv_files.shipments"]["records_valid"] == 1


def test_empty_dataframe_handling(spark, tmp_path):
    """Tests pipeline behavior when a bronze dataset is completely empty."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    rejected_dir = tmp_path / "rejected"
    reports_dir = tmp_path / "reports"

    cust_dir = bronze_dir / "postgres" / "customers" / "2026-11-23"
    cust_dir.mkdir(parents=True)
    cust_file = cust_dir / "customers_emptyrun.csv"
    cust_file.write_text("customer_id,name,email,country,updated_at\n", encoding="utf-8")

    pipeline = DataQualityPipeline(
        spark=spark,
        bronze_dir=str(bronze_dir),
        silver_dir=str(silver_dir),
        rejected_dir=str(rejected_dir),
        reports_dir=str(reports_dir),
    )

    res = pipeline.run_pipeline(run_id="emptyrun")
    assert res["status"] == "SUCCESS"
    assert "postgres.customers" in res["datasets"]
    assert res["datasets"]["postgres.customers"]["records_checked"] == 0
    assert res["datasets"]["postgres.customers"]["records_valid"] == 0
    assert res["datasets"]["postgres.customers"]["records_rejected"] == 0
