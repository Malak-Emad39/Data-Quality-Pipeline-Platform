import random
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text

DB_URI = "postgresql+psycopg2://admin:admin@localhost:5432/ecommerce_dq"
engine = create_engine(DB_URI)

VALID_STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]


def generate_orders(num_valid: int = 100) -> list[dict]:
    records = []
    base_time = datetime.now(timezone.utc)

    for i in range(1, num_valid + 1):
        records.append({
            "order_id": f"ORD-{1000 + i}",
            "customer_id": f"CUST-{random.randint(1, 50):03d}",
            "order_date": (base_time - timedelta(days=random.randint(0, 7))).isoformat(),
            "total_amount": round(random.uniform(20.0, 500.0), 2),
            "status": random.choice(VALID_STATUSES),
        })

    # Fault Injection (حالات اختبار الجودة)
    anomalies = [
        {"order_id": None, "customer_id": "CUST-001", "order_date": base_time.isoformat(), "total_amount": 120.0, "status": "PAID"},
        {"order_id": "ORD-9001", "customer_id": None, "order_date": base_time.isoformat(), "total_amount": 85.5, "status": "PAID"},
        {"order_id": "ORD-9002", "customer_id": "CUST-002", "order_date": base_time.isoformat(), "total_amount": -45.0, "status": "PAID"},
        {"order_id": "ORD-9003", "customer_id": "CUST-003", "order_date": base_time.isoformat(), "total_amount": 0.0, "status": "PENDING"},
        {"order_id": "ORD-9004", "customer_id": "CUST-004", "order_date": base_time.isoformat(), "total_amount": 210.0, "status": "INVALID_STATE"},
        {"order_id": "ORD-9005", "customer_id": "CUST-005", "order_date": base_time.isoformat(), "total_amount": 99.0, "status": "BROKEN"},
        # Duplicates
        {"order_id": "ORD-1001", "customer_id": "CUST-010", "order_date": base_time.isoformat(), "total_amount": 150.0, "status": "PAID"},
        {"order_id": "ORD-1002", "customer_id": "CUST-011", "order_date": base_time.isoformat(), "total_amount": 200.0, "status": "SHIPPED"},
    ]

    records.extend(anomalies)
    return records


def main():
    orders = generate_orders(num_valid=100)
    df = pd.DataFrame(orders)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS orders;"))

    df.to_sql("orders", con=engine, if_exists="replace", index=False)
    print(f"[seed_db] Seeded {len(df)} orders into PostgreSQL.")


if __name__ == "__main__":
    main()