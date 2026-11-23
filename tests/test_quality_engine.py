import pytest

from pyspark.sql import SparkSession

from src.validation.quality_engine import DataQualityEngine


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQualityTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def create_engine():
    return DataQualityEngine(
        run_id="test_001",
        pipeline_name="test_pipeline",
        task_name="data_quality",
    )


def test_all_quality_rules(spark):
    data = [
        (1, "Ali", 25, 10),
        (2, None, 30, 10),
        (3, "Mona", 150, 20),
        (4, "Omar", 22, 99),
        (5, "Sara", 28, 10),
        (5, "Sara", 28, 10),
        (6, "John", 35, 20),
    ]

    columns = ["id", "name", "age", "dept_id"]
    df = spark.createDataFrame(data, columns)

    departments = spark.createDataFrame(
        [(10,), (20,)],
        ["dept_id"],
    )

    valid, rejected, report = create_engine().validate(
        df,
        expected_schema={
            "id": "bigint",
            "name": "string",
            "age": "bigint",
            "dept_id": "bigint",
        },
        required_columns=["name"],
        unique_columns=["id"],
        min_rows=5,
        reference_data={"dept_id": departments},
        range_rules={"age": (18, 100)},
    )

    assert valid.count() == 2
    assert rejected.count() == 5

    assert "run_id" in rejected.columns
    assert "rejection_reason" in rejected.columns

    assert rejected.filter(
        "run_id = 'test_001'"
    ).count() == 5

    report_data = {
        row["check_name"]: row.asDict()
        for row in report.collect()
    }

    expected_rules = {
        "schema",
        "null",
        "duplicate",
        "row_count",
        "referential_integrity",
        "range",
    }

    assert set(report_data.keys()) == expected_rules

    assert report_data["schema"]["check_status"] == "PASSED"
    assert report_data["null"]["check_status"] == "FAILED"
    assert report_data["duplicate"]["check_status"] == "FAILED"
    assert report_data["referential_integrity"]["check_status"] == "FAILED"
    assert report_data["range"]["check_status"] == "FAILED"
    assert report_data["row_count"]["check_status"] == "PASSED"

    # The report should count the actual input records.
    for rule in expected_rules:
        assert report_data[rule]["records_checked"] == 7

    # Successful checks should not contain error messages.
    assert report_data["schema"]["error_message"] is None

    # Failed checks should contain meaningful error messages.
    assert report_data["null"]["error_message"] is not None
    assert "name cannot be NULL" in report_data["null"]["error_message"]


def test_row_count_failure(spark):
    df = spark.createDataFrame(
        [(1, "Ali"), (2, "Mona")],
        ["id", "name"],
    )

    valid, rejected, report = create_engine().validate(
        df,
        min_rows=5,
    )

    row_count = (
        report
        .filter("check_name = 'row_count'")
        .first()
    )

    assert row_count["check_status"] == "FAILED"
    assert row_count["records_checked"] == 2
    assert row_count["records_failed"] == 1
    assert row_count["threshold"] == "minimum rows: 5"

    # Row count is a dataset-level check.
    assert valid.count() == 2
    assert rejected.count() == 0


def test_internal_column_name_conflicts(spark):
    df = spark.createDataFrame(
        [
            (1, "Ali", False, "original"),
            (2, "Mona", False, "original"),
        ],
        [
            "id",
            "name",
            "_dq_duplicate_flag",
            "_quality_errors",
        ],
    )

    valid, rejected, report = create_engine().validate(
        df,
        unique_columns=["id"],
        required_columns=["name"],
    )

    assert valid.count() == 2
    assert rejected.count() == 0

    # Original source columns must remain unchanged.
    assert "_dq_duplicate_flag" in valid.columns
    assert "_quality_errors" in valid.columns

    assert valid.filter(
        "_quality_errors = 'original'"
    ).count() == 2


def test_schema_failure(spark):
    df = spark.createDataFrame(
        [(1, "Ali"), (2, "Mona")],
        ["id", "name"],
    )

    valid, rejected, report = create_engine().validate(
        df,
        expected_schema={
            "id": "string",
            "name": "string",
            "email": "string",
        },
    )

    schema_result = (
        report
        .filter("check_name = 'schema'")
        .first()
    )

    assert schema_result["check_status"] == "FAILED"
    assert "Invalid type for id" in schema_result["error_message"]
    assert "Missing column: email" in schema_result["error_message"]

    assert rejected.count() == 2
    assert valid.count() == 0


from src.validation.contracts import (
    CONTRACTS,
    get_contract,
    CUSTOMERS_CONTRACT,
    PRODUCTS_CONTRACT,
    ORDERS_CONTRACT,
    SHIPMENTS_CONTRACT,
    API_PRODUCTS_CONTRACT,
)


def test_empty_string_validation(spark):
    data = [
        (1, "Valid Name", "DHL"),
        (2, "   ", "Aramex"),  # empty whitespace name
        (3, "", "FedEx"),       # empty string name
        (4, "Omar", ""),        # empty carrier
        (5, "Sara", None),      # null carrier
    ]
    df = spark.createDataFrame(data, ["id", "name", "carrier"])

    valid, rejected, report = create_engine().validate(
        df,
        required_columns=["name", "carrier"],
    )

    # Only row 1 is valid
    assert valid.count() == 1
    assert rejected.count() == 4

    reasons = [row["rejection_reason"] for row in rejected.collect()]
    # Check that empty string errors are reported
    assert any("name cannot be empty" in r for r in reasons)
    assert any("carrier cannot be empty" in r for r in reasons)
    assert any("carrier cannot be NULL" in r for r in reasons)


def test_open_ended_ranges(spark):
    data = [
        (1, 10.5, 5),   # valid
        (2, -9.99, 3),  # invalid price <= 0
        (3, 0.0, 2),    # invalid price <= 0
        (4, 50.0, -1),  # invalid delivery_days < 0
        (5, 25.0, 0),   # valid delivery_days == 0
    ]
    df = spark.createDataFrame(data, ["id", "price", "delivery_days"])

    valid, rejected, report = create_engine().validate(
        df,
        range_rules={
            "price": (0, None, False),         # price > 0 (exclusive)
            "delivery_days": (0, None, True),   # delivery_days >= 0 (inclusive)
        },
    )

    # Valid: rows 1 and 5
    assert valid.count() == 2
    assert rejected.count() == 3

    reasons = [row["rejection_reason"] for row in rejected.collect()]
    assert any("price must be > 0" in r for r in reasons)
    assert any("delivery_days must be >= 0" in r for r in reasons)


def test_date_range_and_future_dates(spark):
    data = [
        (101, "2024-01-15"),  # past date - valid
        (102, "2099-01-01"),  # future date - invalid
        (103, "2026-10-10"),  # past date - valid
    ]
    df = spark.createDataFrame(data, ["order_id", "order_date"])

    valid, rejected, report = create_engine().validate(
        df,
        range_rules={
            "order_date": (None, "current_date"),
        },
    )

    assert valid.count() == 2
    assert rejected.count() == 1

    rejected_row = rejected.first()
    assert rejected_row["order_id"] == 102
    assert "order_date cannot be in the future" in rejected_row["rejection_reason"]


def test_referential_integrity_different_keys(spark):
    orders_data = [
        (1, 10),   # valid
        (2, 20),   # valid
        (3, 999),  # orphan customer
    ]
    orders_df = spark.createDataFrame(orders_data, ["order_id", "cust_fk"])

    customers_data = [
        (10, "Customer A"),
        (20, "Customer B"),
    ]
    customers_df = spark.createDataFrame(customers_data, ["customer_id", "name"])

    # cust_fk in orders references customer_id in customers
    valid, rejected, report = create_engine().validate(
        orders_df,
        reference_data={
            "cust_fk": (customers_df, "customer_id"),
        },
    )

    assert valid.count() == 2
    assert rejected.count() == 1

    rejected_row = rejected.first()
    assert rejected_row["order_id"] == 3
    assert "cust_fk has no matching reference in customer_id" in rejected_row["rejection_reason"]


def test_contracts_definitions():
    assert len(CONTRACTS) == 5
    assert "postgres.customers" in CONTRACTS
    assert "postgres.products" in CONTRACTS
    assert "postgres.orders" in CONTRACTS
    assert "csv_files.shipments" in CONTRACTS
    assert "rest_api.products" in CONTRACTS

    # customers: email required, customer_id unique
    c_contract = get_contract("postgres.customers")
    assert "email" in c_contract.required_columns
    assert "customer_id" in c_contract.unique_columns

    # products: product_id unique, price > 0
    p_contract = get_contract("postgres.products")
    assert "product_id" in p_contract.unique_columns
    assert p_contract.range_rules["price"] == (0, None, False)

    # orders: order_id unique, foreign keys, quantity > 0, amount > 0, order_date not future
    o_contract = get_contract("postgres.orders")
    assert "order_id" in o_contract.unique_columns
    assert "customer_id" in o_contract.foreign_keys
    assert "product_id" in o_contract.foreign_keys
    assert o_contract.range_rules["quantity"] == (0, None, False)
    assert o_contract.range_rules["amount"] == (0, None, False)
    assert o_contract.range_rules["order_date"] == (None, "current_date")

    # shipments: shipment_id unique, order_id FK, carrier required, delivery_days >= 0
    s_contract = get_contract("csv_files.shipments")
    assert "shipment_id" in s_contract.unique_columns
    assert "carrier" in s_contract.required_columns
    assert "order_id" in s_contract.foreign_keys
    assert s_contract.range_rules["delivery_days"] == (0, None, True)

    # api products: stock >= 0, rating between 0 and 5, price > 0
    api_contract = get_contract("rest_api.products")
    assert api_contract.range_rules["stock"] == (0, None, True)
    assert api_contract.range_rules["rating"] == (0.0, 5.0, True)
    assert api_contract.range_rules["price"] == (0, None, False)


def test_safe_cast_prevents_silent_loss(spark):
    from pyspark.sql import functions as F

    data = [
        ("1", "100.50", "2024-01-01"),
        ("invalid_id", "200.00", "2024-01-02"),
        ("3", "corrupt_price", "2024-01-03"),
        ("4", "50.00", "not-a-date"),
    ]
    df = spark.createDataFrame(data, ["id", "price", "order_date"])

    target_schema = {
        "id": "bigint",
        "price": "double",
        "order_date": "date",
    }

    parsed_df, parse_errors = DataQualityEngine.safe_cast(df, target_schema)

    assert len(parse_errors) == 3

    error_col = F.coalesce(*parse_errors)
    evaluated = parsed_df.withColumn("parse_error", error_col)

    errors = evaluated.filter(F.col("parse_error").isNotNull()).collect()
    assert len(errors) == 3
