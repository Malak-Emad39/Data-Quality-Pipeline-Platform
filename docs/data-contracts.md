# Data Contracts & Schemas Specification

## 1. Overview

Data Contracts in this platform (`src/validation/contracts.py`) serve as declarative SLAs between ingestion producers and data quality consumers. Each contract defines:
- The target Bronze table name and Bronze file path pattern.
- The destination Silver and Quarantine storage locations.
- The strict PySpark schema for type casting and validation.
- All nullability, uniqueness, range, date, and referential integrity rules.

Contracts are retrieved programmatically using:
```python
from src.validation.contracts import get_contract

contract = get_contract("postgres.orders")
```

---

## 2. Dataset Contract Specifications

### 2.1. `postgres.customers`

- **Source**: PostgreSQL Table `customers` exported to CSV.
- **Silver Table**: `silver/customers/`
- **Quarantine Table**: `rejected/customers/`
- **Schema**:
  | Column | Spark Type | Nullable | Constraints |
  | :--- | :--- | :--- | :--- |
  | `customer_id` | `LongType` | No | Unique Primary Key |
  | `name` | `StringType` | No | Non-null, non-empty string |
  | `email` | `StringType` | No | Non-null, non-empty, unique |
  | `country` | `StringType` | Yes | Valid country text |
  | `updated_at` | `StringType` | Yes | Ingestion timestamp |
- **Validation Rules**:
  - `duplicate_checks`: Primary Key `['customer_id']` must be strictly unique.
  - `null_checks`: `email` cannot be null or empty string; `name` cannot be null or empty string.

---

### 2.2. `postgres.products`

- **Source**: PostgreSQL Table `products` exported to CSV.
- **Silver Table**: `silver/products/`
- **Quarantine Table**: `rejected/products/`
- **Schema**:
  | Column | Spark Type | Nullable | Constraints |
  | :--- | :--- | :--- | :--- |
  | `product_id` | `LongType` | No | Unique Primary Key |
  | `name` | `StringType` | No | Non-null, non-empty string |
  | `price` | `DoubleType` | No | Numeric, `price > 0.0` |
  | `updated_at` | `StringType` | Yes | Ingestion timestamp |
- **Validation Rules**:
  - `duplicate_checks`: `['product_id']` unique.
  - `null_checks`: `product_id`, `name`, `price` cannot be null.
  - `range_checks`: `price` must be strictly greater than `0.0` (`inclusive_min=False`).

---

### 2.3. `postgres.orders`

- **Source**: PostgreSQL Table `orders` exported to CSV.
- **Silver Table**: `silver/orders/`
- **Quarantine Table**: `rejected/orders/`
- **Schema**:
  | Column | Spark Type | Nullable | Constraints |
  | :--- | :--- | :--- | :--- |
  | `order_id` | `LongType` | No | Unique Primary Key |
  | `customer_id` | `LongType` | No | Foreign Key → Silver `customers.customer_id` |
  | `product_id` | `LongType` | No | Foreign Key → Silver `products.product_id` |
  | `quantity` | `LongType` | No | Integer, `quantity > 0` |
  | `amount` | `DoubleType` | No | Numeric, `amount > 0.0` |
  | `order_date` | `DateType` | No | Date, `order_date <= current_date` |
- **Validation Rules**:
  - `duplicate_checks`: `['order_id']` unique.
  - `null_checks`: `order_id`, `customer_id`, `product_id`, `quantity`, `amount`, `order_date` cannot be null.
  - `referential_integrity`:
    - `customer_id` must match `silver/customers/customer_id`.
    - `product_id` must match `silver/products/product_id`.
  - `range_checks`: `quantity > 0`, `amount > 0.0`.
  - `date_checks`: `order_date` cannot be in the future (`max_value="current_date"`).

---

### 2.4. `csv_files.shipments`

- **Source**: Third-Party Carrier CSV File `sample_shipments.csv`.
- **Silver Table**: `silver/shipments/`
- **Quarantine Table**: `rejected/shipments/`
- **Schema**:
  | Column | Spark Type | Nullable | Constraints |
  | :--- | :--- | :--- | :--- |
  | `shipment_id` | `LongType` | No | Unique Primary Key |
  | `order_id` | `LongType` | No | Foreign Key → Silver `orders.order_id` |
  | `carrier` | `StringType` | No | Non-null, non-empty carrier name |
  | `shipped_date`| `DateType` | Yes | Date format |
  | `delivery_days`| `LongType` | Yes | Integer, `delivery_days >= 0` |
- **Validation Rules**:
  - `duplicate_checks`: `['shipment_id']` unique.
  - `null_checks`: `shipment_id`, `order_id`, `carrier` cannot be null or empty string.
  - `referential_integrity`: `order_id` must match `silver/orders/order_id`.
  - `range_checks`: `delivery_days >= 0` (`inclusive_min=True`).

---

### 2.5. `rest_api.products`

- **Source**: DummyJSON Paginated Catalog REST API (`/products`).
- **Silver Table**: `silver/api_products/`
- **Quarantine Table**: `rejected/api_products/`
- **Schema**:
  | Column | Spark Type | Nullable | Constraints |
  | :--- | :--- | :--- | :--- |
  | `id` | `LongType` | No | Unique Catalog ID |
  | `title` | `StringType` | No | Non-null, non-empty title |
  | `price` | `DoubleType` | No | Numeric, `price >= 0.0` |
  | `stock` | `LongType` | No | Integer, `stock >= 0` |
  | `rating` | `DoubleType` | No | Bounded float, `0.0 <= rating <= 5.0` |
  | `category` | `StringType` | Yes | Category text |
- **Validation Rules**:
  - `duplicate_checks`: `['id']` unique.
  - `null_checks`: `id`, `title`, `price`, `stock`, `rating` cannot be null or empty string.
  - `range_checks`:
    - `price >= 0.0`
    - `stock >= 0`
    - `0.0 <= rating <= 5.0`
