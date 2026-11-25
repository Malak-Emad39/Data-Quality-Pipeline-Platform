from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

VALID_ORDER_STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
VALID_PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "WALLET", "COD"]
VALID_PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Ecommerce-DQ-Bronze-To-Silver")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # ترقيم الصفوف المكررة لنفس order_id
    win = Window.partitionBy("order_id").orderBy(F.col("order_date").asc_nulls_last())
    df_ranked = df.withColumn(
        "_row_num",
        F.when(F.col("order_id").isNotNull(), F.row_number().over(win)).otherwise(F.lit(1)),
    )

    reasons = F.array_compact(
        F.array(
            F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID")),
            F.when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID")),
            F.when(F.col("order_date").isNull(), F.lit("NULL_ORDER_DATE")),
            F.when(F.col("total_amount").isNull() | (F.col("total_amount") <= 0), F.lit("INVALID_AMOUNT")),
            F.when(~F.col("status").isin(VALID_ORDER_STATUSES) | F.col("status").isNull(), F.lit("INVALID_STATUS")),
            F.when(F.col("_row_num") > 1, F.lit("DUPLICATE_ORDER_ID")),
        )
    )

    df_checked = df_ranked.withColumn("_reasons", reasons)

    valid_df = (
        df_checked
        .filter(F.size("_reasons") == 0)
        .drop("_row_num", "_reasons")
        .withColumn("_dq_processed_at", F.current_timestamp())
    )

    rejected_df = (
        df_checked
        .filter(F.size("_reasons") > 0)
        .withColumn("rejection_reason", F.concat_ws(" | ", "_reasons"))
        .withColumn("_rejected_at", F.current_timestamp())
        .drop("_row_num", "_reasons")
    )

    return valid_df, rejected_df


def validate_payments(df: DataFrame, valid_orders_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    win = Window.partitionBy("payment_id").orderBy(F.col("payment_date").asc_nulls_last())
    df_ranked = df.withColumn(
        "_row_num",
        F.when(F.col("payment_id").isNotNull(), F.row_number().over(win)).otherwise(F.lit(1)),
    )

    valid_order_ids = valid_orders_df.select(F.col("order_id").alias("_valid_oid")).distinct()
    df_joined = df_ranked.join(
        valid_order_ids,
        df_ranked["order_id"] == valid_order_ids["_valid_oid"],
        how="left",
    )

    reasons = F.array_compact(
        F.array(
            F.when(F.col("payment_id").isNull(), F.lit("NULL_PAYMENT_ID")),
            F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID")),
            F.when(F.col("order_id").isNotNull() & F.col("_valid_oid").isNull(), F.lit("ORPHAN_ORDER_ID")),
            F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("INVALID_AMOUNT")),
            F.when(~F.col("payment_method").isin(VALID_PAYMENT_METHODS) | F.col("payment_method").isNull(), F.lit("INVALID_PAYMENT_METHOD")),
            F.when(~F.col("payment_status").isin(VALID_PAYMENT_STATUSES) | F.col("payment_status").isNull(), F.lit("INVALID_PAYMENT_STATUS")),
            F.when(F.col("_row_num") > 1, F.lit("DUPLICATE_PAYMENT_ID")),
        )
    )

    df_checked = df_joined.withColumn("_reasons", reasons)

    valid_df = (
        df_checked
        .filter(F.size("_reasons") == 0)
        .drop("_row_num", "_valid_oid", "_reasons")
        .withColumn("_dq_processed_at", F.current_timestamp())
    )

    rejected_df = (
        df_checked
        .filter(F.size("_reasons") > 0)
        .withColumn("rejection_reason", F.concat_ws(" | ", "_reasons"))
        .withColumn("_rejected_at", F.current_timestamp())
        .drop("_row_num", "_valid_oid", "_reasons")
    )

    return valid_df, rejected_df


def print_dq_report(entity: str, total: int, valid: int, rejected_df: DataFrame):
    rejected = rejected_df.count()
    rate = (rejected / total * 100) if total > 0 else 0.0
    print(f"\n{'='*50}")
    print(f"DATA QUALITY REPORT: {entity.upper()}")
    print(f"{'='*50}")
    print(f"Total Records    : {total}")
    print(f"Valid (Silver)   : {valid}")
    print(f"Rejected         : {rejected} ({rate:.2f}%)")
    if rejected > 0:
        print("\nBreakdown by Rejection Reason:")
        rejected_df.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)


def main():
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spark = create_spark_session()

    orders_bronze_path = f"s3a://bronze/orders/ingestion_date={ingestion_date}/raw_orders.json"
    payments_bronze_path = f"s3a://bronze/payments/ingestion_date={ingestion_date}/raw_payments.json"

    raw_orders_df = spark.read.json(orders_bronze_path)
    raw_payments_df = spark.read.json(payments_bronze_path)

    valid_orders_df, rejected_orders_df = validate_orders(raw_orders_df)
    valid_orders_df.cache()

    valid_payments_df, rejected_payments_df = validate_payments(raw_payments_df, valid_orders_df)
    valid_payments_df.cache()

    valid_orders_df.write.mode("overwrite").parquet(f"s3a://silver/orders/ingestion_date={ingestion_date}/")
    rejected_orders_df.write.mode("overwrite").parquet(f"s3a://rejected/orders/ingestion_date={ingestion_date}/")

    valid_payments_df.write.mode("overwrite").parquet(f"s3a://silver/payments/ingestion_date={ingestion_date}/")
    rejected_payments_df.write.mode("overwrite").parquet(f"s3a://rejected/payments/ingestion_date={ingestion_date}/")

    print_dq_report("orders", raw_orders_df.count(), valid_orders_df.count(), rejected_orders_df)
    print_dq_report("payments", raw_payments_df.count(), valid_payments_df.count(), rejected_payments_df)

    spark.stop()


if __name__ == "__main__":
    main()
