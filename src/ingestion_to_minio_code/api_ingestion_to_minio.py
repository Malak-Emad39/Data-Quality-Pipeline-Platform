import json
import random
import time
from datetime import datetime, timezone
import boto3

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BRONZE_BUCKET = "bronze"

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "WALLET", "COD"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def fetch_payments_page(page: int, page_size: int = 25, max_retries: int = 3) -> dict:
    """محاكاة API خارجي يدعم Pagination مع Retry & Exponential Backoff"""
    for attempt in range(1, max_retries + 1):
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            total_valid = 80
            start_idx = (page - 1) * page_size + 1
            end_idx = min(page * page_size, total_valid)

            items = []
            if start_idx <= total_valid:
                for idx in range(start_idx, end_idx + 1):
                    items.append({
                        "payment_id": f"PAY-{2000 + idx}",
                        "order_id": f"ORD-{1000 + idx}",
                        "payment_date": now_iso,
                        "amount": round(random.uniform(20.0, 500.0), 2),
                        "payment_method": random.choice(PAYMENT_METHODS),
                        "payment_status": random.choice(PAYMENT_STATUSES),
                    })

            # في الصفحة الأخيرة نحقن الأخطاء المقصودة (Orphan orders, Nulls, Negative)
            has_next = end_idx < total_valid
            if not has_next:
                items.extend([
                    {"payment_id": "PAY-9001", "order_id": "ORD-NON-EXISTENT-1", "payment_date": now_iso, "amount": 120.0, "payment_method": "CREDIT_CARD", "payment_status": "SUCCESS"},
                    {"payment_id": "PAY-9002", "order_id": "ORD-NON-EXISTENT-2", "payment_date": now_iso, "amount": 75.0, "payment_method": "WALLET", "payment_status": "SUCCESS"},
                    {"payment_id": "PAY-9003", "order_id": "ORD-1001", "payment_date": now_iso, "amount": 50.0, "payment_method": None, "payment_status": "SUCCESS"},
                    {"payment_id": "PAY-9004", "order_id": "ORD-1002", "payment_date": now_iso, "amount": -30.0, "payment_method": "COD", "payment_status": "FAILED"},
                    {"payment_id": None, "order_id": "ORD-1003", "payment_date": now_iso, "amount": 60.0, "payment_method": "DEBIT_CARD", "payment_status": "SUCCESS"},
                ])

            return {"page": page, "items": items, "has_next": has_next}

        except Exception as exc:
            if attempt == max_retries:
                raise
            sleep_sec = 2 ** attempt
            print(f"[extract_api_payments] API transient error ({exc}), retrying in {sleep_sec}s...")
            time.sleep(sleep_sec)


def main():
    now_utc = datetime.now(timezone.utc)
    ingestion_date = now_utc.strftime("%Y-%m-%d")
    ingested_at_iso = now_utc.isoformat()

    all_payments = []
    page = 1
    while True:
        resp = fetch_payments_page(page=page, page_size=25)
        for rec in resp["items"]:
            rec["_ingested_at"] = ingested_at_iso
            rec["_source"] = "payments_gateway_api"
            all_payments.append(rec)
        if not resp["has_next"]:
            break
        page += 1

    key = f"payments/ingestion_date={ingestion_date}/raw_payments.json"
    ndjson_body = "\n".join(json.dumps(r) for r in all_payments)

    s3 = get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=ndjson_body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    print(f"[extract_api_payments] Uploaded {len(all_payments)} payments across {page} pages -> s3a://{BRONZE_BUCKET}/{key}")


if __name__ == "__main__":
    main()