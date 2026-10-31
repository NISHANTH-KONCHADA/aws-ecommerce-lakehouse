"""
Silver Layer Transformation Orchestrator
========================================
Reads raw datasets from the Bronze zone, executes data quality checks and cleaning rules,
routes non-conforming records to Quarantine, and writes standardized Parquet datasets to Silver.

Output Structure:
- Silver Orders: silver/orders/year=YYYY/month=MM/part-*.parquet (Snappy compressed)
- Silver Order Items: silver/order_items/order_items.parquet
- Silver Customers: silver/customers/customers.parquet
- Silver Products: silver/products/products.parquet (Enriched with English categories)
- Silver Payments: silver/payments/payments.parquet
- Silver Reviews: silver/reviews/reviews.parquet
- Silver Sellers: silver/sellers/sellers.parquet

Dual Storage Support:
- Local Mode: Reads from ./data/bronze and writes to ./data/silver.
- S3 Mode: Reads from s3://<bucket>/bronze and writes to s3://<bucket>/silver.

Usage:
    python -m transformations.runner
"""

import argparse
import glob
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

from config.settings import AppConfig, get_config
from data_quality.quarantine import QuarantineManager
from transformations.clean_customers import transform_customers
from transformations.clean_order_items import transform_order_items
from transformations.clean_orders import transform_orders
from transformations.clean_payments import transform_payments
from transformations.clean_products import transform_products
from transformations.clean_reviews import transform_reviews
from transformations.clean_sellers import transform_sellers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transformations.runner")


def load_bronze_df(config: AppConfig, dataset_name: str, bronze_dir: Path) -> pd.DataFrame:
    """
    Finds and reads the latest raw CSV from the Bronze layer for a given dataset.
    """
    dataset_folder = bronze_dir / dataset_name
    if not dataset_folder.exists():
        raise FileNotFoundError(f"Bronze directory not found: {dataset_folder}")

    # Search for all CSV files in subdirectories (e.g. ingestion_date=YYYY-MM-DD/*.csv)
    csv_files = sorted(dataset_folder.glob("**/*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No Bronze CSV files found in {dataset_folder}")

    # Read the latest ingested file
    latest_file = csv_files[-1]
    logger.info(f"Loading Bronze data from: {latest_file.relative_to(config.storage.local_data_dir)}")
    return pd.read_csv(latest_file, dtype=str)


def write_silver_parquet(
    df: pd.DataFrame,
    table_name: str,
    config: AppConfig,
    partition_cols: Optional[list] = None
) -> str:
    """
    Writes a cleaned DataFrame to the Silver zone in Snappy-compressed Parquet format.
    Supports partition columns (e.g., year, month).
    """
    if config.storage.storage_mode.lower() == "s3":
        target_uri = config.get_zone_path("silver", table_name)
        storage_options = {}
        if config.aws_access_key_id and config.aws_secret_access_key:
            storage_options = {
                "key": config.aws_access_key_id,
                "secret": config.aws_secret_access_key,
                "client_kwargs": {"region_name": config.aws_region}
            }

        logger.info(f"Writing Parquet to S3: {target_uri}")
        if partition_cols:
            df.to_parquet(
                target_uri,
                engine="pyarrow",
                compression="snappy",
                partition_cols=partition_cols,
                index=False,
                storage_options=storage_options
            )
        else:
            file_uri = f"{target_uri}/{table_name}.parquet"
            df.to_parquet(
                file_uri,
                engine="pyarrow",
                compression="snappy",
                index=False,
                storage_options=storage_options
            )
        return target_uri
    else:
        target_dir = Path(config.get_zone_path("silver", table_name))
        target_dir.mkdir(parents=True, exist_ok=True)

        if partition_cols:
            df.to_parquet(
                target_dir,
                engine="pyarrow",
                compression="snappy",
                partition_cols=partition_cols,
                index=False
            )
        else:
            file_path = target_dir / f"{table_name}.parquet"
            df.to_parquet(file_path, engine="pyarrow", compression="snappy", index=False)

        logger.info(f"✓ Wrote Silver Parquet to: {target_dir}")
        return str(target_dir)


def run_silver_transformations() -> bool:
    """Orchestrates all Bronze to Silver transformations."""
    config = get_config()
    run_id = str(uuid.uuid4())
    quarantine_manager = QuarantineManager(run_id=run_id, config=config)

    logger.info("=" * 75)
    logger.info("STARTING SILVER TRANSFORMATIONS (BRONZE -> SILVER PARQUET)")
    logger.info(f"Storage Mode: {config.storage.storage_mode.upper()}")
    logger.info(f"Pipeline Run ID: {run_id}")
    logger.info("=" * 75)

    bronze_dir = Path(config.get_zone_path("bronze"))

    # [1] Category Translation (Lookup Table)
    translations_df = None
    try:
        raw_trans = load_bronze_df(config, "category_translation", bronze_dir)
        translations_df = raw_trans.dropna().drop_duplicates()
        write_silver_parquet(translations_df, "category_translation", config)
    except Exception as e:
        logger.warning(f"Category translations lookup unavailable: {e}")

    # [2] Orders
    raw_orders = load_bronze_df(config, "orders", bronze_dir)
    clean_orders, _ = transform_orders(raw_orders, quarantine_manager)
    write_silver_parquet(clean_orders, "orders", config, partition_cols=["year", "month"])

    # [3] Order Items
    raw_items = load_bronze_df(config, "order_items", bronze_dir)
    clean_items, _ = transform_order_items(raw_items, quarantine_manager)
    write_silver_parquet(clean_items, "order_items", config)

    # [4] Payments
    raw_payments = load_bronze_df(config, "payments", bronze_dir)
    clean_payments, _ = transform_payments(raw_payments, quarantine_manager)
    write_silver_parquet(clean_payments, "payments", config)

    # [5] Customers
    raw_customers = load_bronze_df(config, "customers", bronze_dir)
    clean_customers, _ = transform_customers(raw_customers, quarantine_manager)
    write_silver_parquet(clean_customers, "customers", config)

    # [6] Products
    raw_products = load_bronze_df(config, "products", bronze_dir)
    clean_products, _ = transform_products(raw_products, translations_df, quarantine_manager)
    write_silver_parquet(clean_products, "products", config)

    # [7] Sellers
    raw_sellers = load_bronze_df(config, "sellers", bronze_dir)
    clean_sellers, _ = transform_sellers(raw_sellers, quarantine_manager)
    write_silver_parquet(clean_sellers, "sellers", config)

    # [8] Reviews
    raw_reviews = load_bronze_df(config, "reviews", bronze_dir)
    clean_reviews, _ = transform_reviews(raw_reviews, quarantine_manager)
    write_silver_parquet(clean_reviews, "reviews", config)

    # Output Quality Scorecard
    quarantine_manager.print_quality_scorecard()
    logger.info("Silver transformations completed successfully.")
    return True


if __name__ == "__main__":
    run_silver_transformations()
