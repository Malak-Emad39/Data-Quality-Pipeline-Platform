import json
from datetime import datetime, timezone
import boto3
import pandas as pd
from sqlalchemy import create_engine

DB_URI = "postgresql+psycopg2://admin:admin@localhost:5432/ecommerce_dq"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BRONZE_BUCKET = "bronze"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def main():
    now_utc = datetime.now(timezone.utc)
    ingestion_date = now_utc.strftime("%Y-%m-%d")
    ingested_at_iso = now_utc.isoformat()

    engine = create_engine(DB_URI)
    df = pd.read_sql("SELECT * FROM orders", con=engine)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    for rec in records:
        rec["_ingested_at"] = ingested_at_iso
        rec["_source"] = "postgres_orders_db"

    key = f"orders/ingestion_date={ingestion_date}/raw_orders.json"
    ndjson_body = "\n".join(json.dumps(r) for r in records)

    s3 = get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=ndjson_body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    print(f"[extract_db_orders] Uploaded {len(records)} orders -> s3a://{BRONZE_BUCKET}/{key}")


if __name__ == "__main__":
    main()