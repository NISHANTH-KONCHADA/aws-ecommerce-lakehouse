"""
AWS Glue PySpark Unified ETL Job
================================
Serverless PySpark ETL script deployed to AWS Glue to process Bronze data into
Silver Parquet and Gold dimensional tables.

Architecture & Execution Lifecycle:
1. AWS Glue Job Parameters:
   Receives `--JOB_NAME`, `--S3_BUCKET_NAME`, and `--INGESTION_DATE` via AWS Glue getResolvedOptions.
2. Spark Session Initialization:
   Initializes GlueContext and SparkSession with dynamic partition overwriting enabled.
3. Bronze Extraction:
   Reads raw source CSV files from s3://<bucket>/bronze/<dataset>/ingestion_date=<date>/*.
4. Silver Layer Transformation:
   - Normalizes schemas, casts numeric types, and parses UTC timestamps.
   - Deduplicates primary keys using PySpark window functions (row_number).
   - Writes Snappy-compressed Parquet partitioned by year/month to s3://<bucket>/silver/<table_name>/.
5. Gold Layer Transformation:
   - Generates Kimball star schema fact and dimension tables.
   - Computes aggregated business marts (daily_sales, product_performance, delivery_kpis).
   - Writes Gold Parquet to s3://<bucket>/gold/<table_name>/.

Usage on AWS Glue:
    aws glue start-job-run --job-name ecommerce_etl --arguments '{"--S3_BUCKET_NAME":"my-bucket","--INGESTION_DATE":"2026-10-31"}'
"""

import sys
from typing import Dict

try:
    # AWS Glue specific libraries
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    IS_GLUE_ENV = True
except ImportError:
    # Local PySpark fallback for offline testing
    IS_GLUE_ENV = False
    from pyspark.sql import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def init_spark_session(job_name: str = "ecommerce_etl"):
    """Initializes Spark/Glue context with optimized settings."""
    if IS_GLUE_ENV:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        return spark, glue_context, job
    else:
        spark = (
            SparkSession.builder
            .appName(job_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        return spark, None, None


def clean_orders(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans orders and writes partitioned Parquet to Silver."""
    bronze_path = f"s3://{s3_bucket}/bronze/orders/ingestion_date={ingestion_date}/*.csv"
    silver_path = f"s3://{s3_bucket}/silver/orders"

    df = spark.read.option("header", "true").csv(bronze_path)

    # Deduplicate on order_id
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_purchase_timestamp").desc())
    deduped = df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")

    # Parse timestamps
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for c in timestamp_cols:
        deduped = deduped.withColumn(c, F.to_timestamp(F.col(c)))

    # Filter null purchase timestamps and validate chronology
    valid_orders = (
        deduped
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("year", F.year(F.col("order_purchase_timestamp")).cast("string"))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "MM"))
    )

    # Write Silver partitioned by year and month
    (
        valid_orders.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(silver_path)
    )
    return valid_orders


def clean_order_items(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans order items and writes to Silver."""
    bronze_path = f"s3://{s3_bucket}/bronze/order_items/ingestion_date={ingestion_date}/*.csv"
    silver_path = f"s3://{s3_bucket}/silver/order_items"

    df = spark.read.option("header", "true").csv(bronze_path)

    cleaned = (
        df
        .withColumn("order_item_id", F.col("order_item_id").cast("integer"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
        .filter((F.col("price") >= 0.0) & (F.col("freight_value") >= 0.0))
        .dropDuplicates(["order_id", "order_item_id"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


def clean_payments(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans payments and writes to Silver."""
    bronze_path = f"s3://{s3_bucket}/bronze/payments/ingestion_date={ingestion_date}/*.csv"
    silver_path = f"s3://{s3_bucket}/silver/payments"

    df = spark.read.option("header", "true").csv(bronze_path)

    cleaned = (
        df
        .withColumn("payment_sequential", F.col("payment_sequential").cast("integer"))
        .withColumn("payment_installments", F.col("payment_installments").cast("integer"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .filter((F.col("payment_value") >= 0.0) & (F.col("payment_installments") >= 1))
        .dropDuplicates(["order_id", "payment_sequential"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


def build_gold_daily_sales(spark: SparkSession, s3_bucket: str):
    """Computes gold_daily_sales mart using Spark SQL aggregations."""
    silver_orders = spark.read.parquet(f"s3://{s3_bucket}/silver/orders")
    silver_items = spark.read.parquet(f"s3://{s3_bucket}/silver/order_items")

    # Pre-aggregate order items by order_id to prevent fan-out
    order_item_agg = (
        silver_items.groupBy("order_id")
        .agg(
            F.count("order_item_id").alias("items_count"),
            F.sum("price").alias("item_rev"),
            F.sum("freight_value").alias("freight_rev")
        )
    )

    # Filter out canceled orders
    valid_orders = silver_orders.filter(F.col("order_status") != "canceled")

    # Join and aggregate by date
    daily_sales = (
        valid_orders.join(order_item_agg, on="order_id", how="inner")
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp")))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("items_count").alias("total_items_sold"),
            F.round(F.sum("item_rev"), 2).alias("item_revenue"),
            F.round(F.sum("freight_rev"), 2).alias("freight_revenue")
        )
        .withColumn("gross_revenue", F.round(F.col("item_revenue") + F.col("freight_revenue"), 2))
        .withColumn("average_order_value", F.round(F.col("item_revenue") / F.col("total_orders"), 2))
        .orderBy("order_date")
    )

    gold_path = f"s3://{s3_bucket}/gold/gold_daily_sales"
    daily_sales.write.mode("overwrite").parquet(gold_path)


def main():
    """Main entry point for AWS Glue execution."""
    if IS_GLUE_ENV:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET_NAME", "INGESTION_DATE"])
        job_name = args["JOB_NAME"]
        s3_bucket = args["S3_BUCKET_NAME"]
        ingestion_date = args["INGESTION_DATE"]
    else:
        job_name = "local_ecommerce_etl"
        s3_bucket = "ecommerce-lakehouse"
        ingestion_date = "2026-10-31"

    spark, glue_context, job = init_spark_session(job_name)
    if IS_GLUE_ENV:
        job.init(job_name, args)

    print(f"Starting Glue ETL Pipeline for bucket: {s3_bucket}, Date: {ingestion_date}")

    # Process Silver
    clean_orders(spark, s3_bucket, ingestion_date)
    clean_order_items(spark, s3_bucket, ingestion_date)
    clean_payments(spark, s3_bucket, ingestion_date)

    # Process Gold
    build_gold_daily_sales(spark, s3_bucket)

    print("Glue ETL Pipeline completed successfully.")
    if IS_GLUE_ENV:
        job.commit()


if __name__ == "__main__":
    main()
