"""
AWS Glue PySpark Unified ETL Job
================================
Serverless PySpark ETL script deployed to AWS Glue to process Bronze data into
Silver Parquet and Gold dimensional tables.

Resilient Data Resolution:
Automatically checks multiple common S3 landing prefixes:
1. Date-partitioned: `s3://<bucket>/bronze/<dataset>/ingestion_date=<date>/*.csv`
2. Direct folder: `s3://<bucket>/bronze/<dataset>/*.csv`
3. Flat root folder: `s3://<bucket>/bronze/<file_name>.csv` or `s3://<bucket>/dataset/<file_name>.csv`
"""

import sys
from typing import Dict, List, Optional

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


def init_spark_session(job_name: str = "ecommerce_batch_etl"):
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


def resolve_s3_input_path(spark: SparkSession, s3_bucket: str, dataset_name: str, file_name: str, ingestion_date: str) -> str:
    """
    Checks common S3 prefixes to find where the user uploaded the raw CSV file.
    Returns the first path that successfully loads, or raises a clear, helpful error.
    """
    candidate_paths = [
        f"s3://{s3_bucket}/bronze/{dataset_name}/ingestion_date={ingestion_date}/*.csv",
        f"s3://{s3_bucket}/bronze/{dataset_name}/{file_name}",
        f"s3://{s3_bucket}/bronze/{file_name}",
        f"s3://{s3_bucket}/dataset/{file_name}",
        f"s3://{s3_bucket}/raw/{file_name}",
        f"s3://{s3_bucket}/{file_name}"
    ]

    for path in candidate_paths:
        try:
            test_df = spark.read.option("header", "true").csv(path)
            if test_df.take(1):
                print(f"✓ Found dataset [{dataset_name}] at: {path}")
                return path
        except Exception:
            continue

    raise FileNotFoundError(
        f"Could not find [{file_name}] in S3 bucket '{s3_bucket}'. "
        f"Checked paths: {candidate_paths}. "
        f"Please upload '{file_name}' to s3://{s3_bucket}/bronze/{dataset_name}/ingestion_date={ingestion_date}/ or s3://{s3_bucket}/bronze/{file_name}"
    )


# ==============================================================================
# SILVER TRANSFORMATIONS
# ==============================================================================

def clean_orders(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans orders and writes partitioned Parquet to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "orders", "olist_orders_dataset.csv", ingestion_date)
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

    valid_orders = (
        deduped
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("year", F.year(F.col("order_purchase_timestamp")).cast("string"))
        .withColumn("month", F.date_format(F.col("order_purchase_timestamp"), "MM"))
    )

    valid_orders.write.mode("overwrite").partitionBy("year", "month").parquet(silver_path)
    return valid_orders


def clean_order_items(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans order items and writes to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "order_items", "olist_order_items_dataset.csv", ingestion_date)
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
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "payments", "olist_order_payments_dataset.csv", ingestion_date)
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


def clean_customers(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans customers and writes to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "customers", "olist_customers_dataset.csv", ingestion_date)
    silver_path = f"s3://{s3_bucket}/silver/customers"

    df = spark.read.option("header", "true").csv(bronze_path)

    cleaned = (
        df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("customer_zip_code_prefix", F.lpad(F.col("customer_zip_code_prefix"), 5, "0"))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
        .dropDuplicates(["customer_id"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


def clean_products(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans products, enriches with English translation, and writes to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "products", "olist_products_dataset.csv", ingestion_date)
    silver_path = f"s3://{s3_bucket}/silver/products"

    df = spark.read.option("header", "true").csv(bronze_path)

    # Attempt to load translation mapping
    try:
        trans_path = resolve_s3_input_path(spark, s3_bucket, "category_translation", "product_category_name_translation.csv", ingestion_date)
        trans_df = spark.read.option("header", "true").csv(trans_path).dropDuplicates(["product_category_name"])
        df = df.join(trans_df, on="product_category_name", how="left")
    except Exception:
        df = df.withColumn("product_category_name_english", F.col("product_category_name"))

    cleaned = (
        df
        .filter(F.col("product_id").isNotNull())
        .withColumn("product_category_name", F.coalesce(F.col("product_category_name"), F.lit("unknown")))
        .withColumn("product_category_name_english", F.coalesce(F.col("product_category_name_english"), F.lit("unknown")))
        .withColumn("product_weight_g", F.col("product_weight_g").cast("double"))
        .withColumn("product_length_cm", F.col("product_length_cm").cast("double"))
        .withColumn("product_height_cm", F.col("product_height_cm").cast("double"))
        .withColumn("product_width_cm", F.col("product_width_cm").cast("double"))
        .dropDuplicates(["product_id"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


def clean_sellers(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans sellers and writes to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "sellers", "olist_sellers_dataset.csv", ingestion_date)
    silver_path = f"s3://{s3_bucket}/silver/sellers"

    df = spark.read.option("header", "true").csv(bronze_path)

    cleaned = (
        df
        .filter(F.col("seller_id").isNotNull())
        .withColumn("seller_zip_code_prefix", F.lpad(F.col("seller_zip_code_prefix"), 5, "0"))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .dropDuplicates(["seller_id"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


def clean_reviews(spark: SparkSession, s3_bucket: str, ingestion_date: str):
    """Cleans reviews, validates score range (1-5), and writes to Silver."""
    bronze_path = resolve_s3_input_path(spark, s3_bucket, "reviews", "olist_order_reviews_dataset.csv", ingestion_date)
    silver_path = f"s3://{s3_bucket}/silver/reviews"

    df = spark.read.option("header", "true").csv(bronze_path)

    cleaned = (
        df
        .filter(F.col("review_id").isNotNull() & F.col("order_id").isNotNull())
        .withColumn("review_score", F.col("review_score").cast("integer"))
        .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
        .withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date")))
        .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp")))
        .withColumn("review_comment_title", F.coalesce(F.trim(F.col("review_comment_title")), F.lit("")))
        .withColumn("review_comment_message", F.coalesce(F.trim(F.col("review_comment_message")), F.lit("")))
        .dropDuplicates(["review_id"])
    )

    cleaned.write.mode("overwrite").parquet(silver_path)
    return cleaned


# ==============================================================================
# GOLD MODELING (DIMENSIONS, FACTS & BUSINESS MARTS)
# ==============================================================================

def build_gold_dimensions_and_facts(spark: SparkSession, s3_bucket: str):
    """Builds Kimball Star Schema Dimensions and Fact Tables in Gold."""
    silver_orders = spark.read.parquet(f"s3://{s3_bucket}/silver/orders")
    silver_items = spark.read.parquet(f"s3://{s3_bucket}/silver/order_items")
    silver_payments = spark.read.parquet(f"s3://{s3_bucket}/silver/payments")
    silver_customers = spark.read.parquet(f"s3://{s3_bucket}/silver/customers")
    silver_products = spark.read.parquet(f"s3://{s3_bucket}/silver/products")
    silver_sellers = spark.read.parquet(f"s3://{s3_bucket}/silver/sellers")

    # 1. dim_customers
    dim_cust = silver_customers.select(
        "customer_id", "customer_unique_id", "customer_city", "customer_state", "customer_zip_code_prefix"
    ).dropDuplicates(["customer_id"])
    dim_cust.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/dim_customers")

    # 2. dim_products
    dim_prod = silver_products.select(
        "product_id", "product_category_name", "product_category_name_english",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ).dropDuplicates(["product_id"])
    dim_prod.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/dim_products")

    # 3. dim_sellers
    dim_sell = silver_sellers.select(
        "seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"
    ).dropDuplicates(["seller_id"])
    dim_sell.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/dim_sellers")

    # 4. fact_order_items
    orders_ctx = silver_orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
    fact_items = (
        silver_items.join(orders_ctx, on="order_id", how="inner")
        .withColumn("order_date_key", F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("integer"))
        .withColumn("total_item_value", F.round(F.col("price") + F.col("freight_value"), 2))
    )
    fact_items.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/fact_order_items")

    # 5. fact_payments
    fact_payments = (
        silver_payments.join(orders_ctx, on="order_id", how="inner")
        .withColumn("order_date_key", F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("integer"))
    )
    fact_payments.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/fact_payments")


def build_gold_analytics_marts(spark: SparkSession, s3_bucket: str):
    """Computes daily_sales, product_performance, and delivery_kpis in Gold."""
    silver_orders = spark.read.parquet(f"s3://{s3_bucket}/silver/orders")
    silver_items = spark.read.parquet(f"s3://{s3_bucket}/silver/order_items")
    silver_customers = spark.read.parquet(f"s3://{s3_bucket}/silver/customers")
    dim_products = spark.read.parquet(f"s3://{s3_bucket}/gold/dim_products")

    # --- Mart 1: Daily Sales ---
    order_item_agg = (
        silver_items.groupBy("order_id")
        .agg(
            F.count("order_item_id").alias("items_count"),
            F.sum("price").alias("item_rev"),
            F.sum("freight_value").alias("freight_rev")
        )
    )
    valid_orders = silver_orders.filter(F.col("order_status") != "canceled")
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
    daily_sales.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/gold_daily_sales")

    # --- Mart 2: Product Performance ---
    items_with_cat = silver_items.join(
        dim_products.select("product_id", "product_category_name_english"),
        on="product_id",
        how="left"
    )
    prod_perf = (
        items_with_cat.groupBy("product_category_name_english")
        .agg(
            F.count("order_item_id").alias("units_sold"),
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.countDistinct("seller_id").alias("unique_sellers")
        )
        .withColumn("avg_unit_price", F.round(F.col("total_revenue") / F.col("units_sold"), 2))
        .orderBy(F.col("total_revenue").desc())
    )
    prod_perf.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/gold_product_performance")

    # --- Mart 3: Delivery KPIs ---
    delivered = (
        silver_orders
        .filter((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull())
        .join(silver_customers.select("customer_id", "customer_state"), on="customer_id", how="inner")
        .withColumn("delivery_duration_days", (F.unix_timestamp("order_delivered_customer_date") - F.unix_timestamp("order_purchase_timestamp")) / 86400.0)
        .withColumn("is_late", F.when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1).otherwise(0))
        .withColumn("order_month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
    )
    delivery_kpis = (
        delivered.groupBy("customer_state", "order_month")
        .agg(
            F.count("order_id").alias("total_delivered_orders"),
            F.sum("is_late").alias("late_orders_count"),
            F.round(F.avg("delivery_duration_days"), 2).alias("avg_delivery_duration_days")
        )
        .withColumn("late_delivery_rate_pct", F.round(F.col("late_orders_count") * 100.0 / F.col("total_delivered_orders"), 2))
        .orderBy("customer_state", "order_month")
    )
    delivery_kpis.write.mode("overwrite").parquet(f"s3://{s3_bucket}/gold/gold_delivery_kpis")


def main():
    """Main entry point for AWS Glue execution."""
    if IS_GLUE_ENV:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET_NAME", "INGESTION_DATE"])
        job_name = args["JOB_NAME"]
        s3_bucket = args["S3_BUCKET_NAME"]
        ingestion_date = args["INGESTION_DATE"]
    else:
        job_name = "local_ecommerce_etl"
        s3_bucket = "ecom-lakehouse-nishanth-7"
        ingestion_date = "2026-10-31"

    spark, glue_context, job = init_spark_session(job_name)
    if IS_GLUE_ENV:
        job.init(job_name, args)

    print(f"Starting Glue ETL Pipeline for bucket: {s3_bucket}, Date: {ingestion_date}")

    # 1. Clean Silver Layer
    print("Processing Silver Layer...")
    clean_orders(spark, s3_bucket, ingestion_date)
    clean_order_items(spark, s3_bucket, ingestion_date)
    clean_payments(spark, s3_bucket, ingestion_date)
    clean_customers(spark, s3_bucket, ingestion_date)
    clean_products(spark, s3_bucket, ingestion_date)
    clean_sellers(spark, s3_bucket, ingestion_date)
    clean_reviews(spark, s3_bucket, ingestion_date)

    # 2. Build Gold Star Schema & Business Marts
    print("Processing Gold Dimensions and Facts...")
    build_gold_dimensions_and_facts(spark, s3_bucket)

    print("Processing Gold Analytics Marts...")
    build_gold_analytics_marts(spark, s3_bucket)

    print("✓ Glue ETL Pipeline completed successfully!")
    if IS_GLUE_ENV:
        job.commit()


if __name__ == "__main__":
    main()
