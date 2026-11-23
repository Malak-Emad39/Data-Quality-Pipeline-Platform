"""
Data Contracts Definition Module for DEPI Data Quality ETL Pipeline Platform.

Defines schemas, constraints, and validation rules for all Bronze data sources:
- postgres.customers
- postgres.products
- postgres.orders
- csv_files.shipments
- rest_api.products
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any


@dataclass
class DataContract:
    """Represents a formal data contract and quality expectations for a dataset."""
    dataset_name: str
    source_type: str  # 'postgres', 'csv_files', 'rest_api'
    expected_schema: Dict[str, str]
    required_columns: List[str]
    unique_columns: List[str]
    range_rules: Dict[str, Any] = field(default_factory=dict)
    foreign_keys: Dict[str, Any] = field(default_factory=dict)
    min_rows: Optional[int] = 1
    description: str = ""

    def get_validation_kwargs(self, reference_dfs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Converts the contract into keyword arguments for DataQualityEngine.validate().
        Allows passing resolved reference DataFrames for foreign keys.
        """
        resolved_refs = {}
        if reference_dfs and self.foreign_keys:
            for fk_col, ref_info in self.foreign_keys.items():
                ref_dataset = ref_info.get("reference_dataset")
                ref_col = ref_info.get("reference_column", fk_col)
                if ref_dataset in reference_dfs:
                    resolved_refs[fk_col] = (reference_dfs[ref_dataset], ref_col)

        return {
            "expected_schema": self.expected_schema,
            "required_columns": self.required_columns,
            "unique_columns": self.unique_columns,
            "min_rows": self.min_rows,
            "reference_data": resolved_refs,
            "range_rules": self.range_rules,
        }


# 1. postgres.customers
# Schema: customer_id INT, name TEXT, email TEXT, country TEXT, updated_at TIMESTAMP
# Rules: email required, customer_id unique, customer_id required.
CUSTOMERS_CONTRACT = DataContract(
    dataset_name="postgres.customers",
    source_type="postgres",
    description="Customer master records ingested from PostgreSQL operational database.",
    expected_schema={
        "customer_id": "bigint",
        "name": "string",
        "email": "string",
        "country": "string",
        "updated_at": "timestamp",
    },
    required_columns=["customer_id", "name", "email"],
    unique_columns=["customer_id"],
    min_rows=1,
)

# 2. postgres.products
# Schema: product_id INT, name TEXT, price NUMERIC(10,2), updated_at TIMESTAMP
# Rules: product_id unique, product_id and name required, price must be greater than zero.
PRODUCTS_CONTRACT = DataContract(
    dataset_name="postgres.products",
    source_type="postgres",
    description="Product catalog ingested from PostgreSQL operational database.",
    expected_schema={
        "product_id": "bigint",
        "name": "string",
        "price": "double",
        "updated_at": "timestamp",
    },
    required_columns=["product_id", "name", "price"],
    unique_columns=["product_id"],
    range_rules={
        # price must be greater than zero (> 0, exclusive minimum 0)
        "price": (0, None, False),
    },
    min_rows=1,
)

# 3. postgres.orders
# Schema: order_id INT, customer_id INT, product_id INT, quantity INT, amount NUMERIC(10,2),
#         order_date DATE, updated_at TIMESTAMP
# Rules: order_id unique, required foreign keys, valid customer/product references,
#        quantity and amount greater than zero, order_date cannot be in the future.
ORDERS_CONTRACT = DataContract(
    dataset_name="postgres.orders",
    source_type="postgres",
    description="Order transactions ingested from PostgreSQL operational database.",
    expected_schema={
        "order_id": "bigint",
        "customer_id": "bigint",
        "product_id": "bigint",
        "quantity": "bigint",
        "amount": "double",
        "order_date": "date",
        "updated_at": "timestamp",
    },
    required_columns=[
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "order_date",
    ],
    unique_columns=["order_id"],
    range_rules={
        # quantity must be greater than zero (> 0)
        "quantity": (0, None, False),
        # amount must be greater than zero (> 0)
        "amount": (0, None, False),
        # order_date cannot be in the future
        "order_date": (None, "current_date"),
    },
    foreign_keys={
        "customer_id": {
            "reference_dataset": "postgres.customers",
            "reference_column": "customer_id",
        },
        "product_id": {
            "reference_dataset": "postgres.products",
            "reference_column": "product_id",
        },
    },
    min_rows=1,
)

# 4. csv_files.shipments
# Schema: shipment_id INT, order_id INT, carrier TEXT, shipped_date DATE, delivery_days INT
# Rules: shipment_id unique, order_id references valid order, carrier cannot be NULL or empty,
#        delivery_days must be non-negative (>= 0).
SHIPMENTS_CONTRACT = DataContract(
    dataset_name="csv_files.shipments",
    source_type="csv_files",
    description="Shipment tracking events ingested from batch CSV file.",
    expected_schema={
        "shipment_id": "bigint",
        "order_id": "bigint",
        "carrier": "string",
        "shipped_date": "date",
        "delivery_days": "bigint",
    },
    required_columns=[
        "shipment_id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivery_days",
    ],
    unique_columns=["shipment_id"],
    range_rules={
        # delivery_days must be non-negative (>= 0, inclusive minimum 0)
        "delivery_days": (0, None, True),
    },
    foreign_keys={
        "order_id": {
            "reference_dataset": "postgres.orders",
            "reference_column": "order_id",
        },
    },
    min_rows=1,
)

# 5. rest_api.products
# Schema (from DummyJSON products): id, title, category, price, stock, rating, etc.
# Rules: id unique, title/price/stock/rating required, stock >= 0, rating between 0 and 5, price > 0.
API_PRODUCTS_CONTRACT = DataContract(
    dataset_name="rest_api.products",
    source_type="rest_api",
    description="Product details retrieved from DummyJSON REST API.",
    expected_schema={
        "id": "bigint",
        "title": "string",
        "category": "string",
        "price": "double",
        "rating": "double",
        "stock": "bigint",
    },
    required_columns=["id", "title", "price", "stock", "rating"],
    unique_columns=["id"],
    range_rules={
        # stock >= 0 (inclusive)
        "stock": (0, None, True),
        # rating between 0 and 5
        "rating": (0.0, 5.0, True),
        # price > 0 (exclusive)
        "price": (0, None, False),
    },
    min_rows=1,
)

# Registry of all data contracts
CONTRACTS: Dict[str, DataContract] = {
    "postgres.customers": CUSTOMERS_CONTRACT,
    "postgres.products": PRODUCTS_CONTRACT,
    "postgres.orders": ORDERS_CONTRACT,
    "csv_files.shipments": SHIPMENTS_CONTRACT,
    "rest_api.products": API_PRODUCTS_CONTRACT,
}


def get_contract(dataset_name: str) -> DataContract:
    """Retrieve contract by dataset name."""
    if dataset_name not in CONTRACTS:
        raise KeyError(
            f"No data contract found for '{dataset_name}'. "
            f"Available contracts: {list(CONTRACTS.keys())}"
        )
    return CONTRACTS[dataset_name]
