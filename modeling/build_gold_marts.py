"""
Gold Layer Analytics Marts Builder
==================================
Builds business-ready aggregated analytical tables and Star Schema tables in the Gold zone.

Curated Marts Created:
1. gold_daily_sales:
   - Grain: One row per calendar date (order_date).
   - Metrics: order_count, total_items_sold, item_revenue, freight_revenue,
     gross_revenue, average_order_value (AOV).
2. gold_product_performance:
   - Grain: One row per English product category.
   - Metrics: total_units_sold, total_item_revenue, avg_unit_price, unique_sellers.
3. gold_delivery_kpis:
   - Grain: One row per customer_state and order_month.
   - Metrics: total_delivered_orders, late_orders_count, late_delivery_rate_pct,
     avg_delivery_duration_days.

Why Pre-Aggregating in Gold Matters:
Executive dashboards and recurring BI queries query Gold marts directly in Athena,
scanning megabytes instead of gigabytes, achieving sub-second responses and near-zero cost.
"""

import logging
from pathlib import Path
from typing import Dict
import pandas as pd

from config.settings import AppConfig, get_config
from modeling.dim_tables import build_dim_customers, build_dim_date, build_dim_products, build_dim_sellers
from modeling.fact_tables import build_fact_order_items, build_fact_payments

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("modeling.gold")


def build_daily_sales_mart(fact_items_df: pd.DataFrame, silver_orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the daily sales performance mart by safe aggregation.
    Avoids fan-out by aggregating items to order level before joining.
    """
    logger.info("Computing gold_daily_sales mart...")

    # [Step 1] Aggregate items per order_id
    order_item_agg = fact_items_df.groupby("order_id").agg(
        items_in_order=("order_item_id", "count"),
        order_item_revenue=("price", "sum"),
        order_freight_revenue=("freight_value", "sum")
    ).reset_index()

    # [Step 2] Join with orders to get purchase date and filter out canceled orders
    orders_valid = silver_orders_df[silver_orders_df["order_status"] != "canceled"].copy()
    orders_valid["order_date"] = pd.to_datetime(orders_valid["order_purchase_timestamp"]).dt.date

    merged = orders_valid.merge(order_item_agg, on="order_id", how="inner")

    # [Step 3] Aggregate by date
    daily = merged.groupby("order_date").agg(
        total_orders=("order_id", "nunique"),
        total_items_sold=("items_in_order", "sum"),
        item_revenue=("order_item_revenue", "sum"),
        freight_revenue=("order_freight_revenue", "sum")
    ).reset_index()

    daily["gross_revenue"] = (daily["item_revenue"] + daily["freight_revenue"]).round(2)
    daily["item_revenue"] = daily["item_revenue"].round(2)
    daily["freight_revenue"] = daily["freight_revenue"].round(2)
    daily["average_order_value"] = (daily["item_revenue"] / daily["total_orders"]).round(2)

    # Sort chronologically
    return daily.sort_values("order_date").reset_index(drop=True)


def build_product_performance_mart(
    fact_items_df: pd.DataFrame,
    dim_products_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Builds product category sales and volume analytics.
    """
    logger.info("Computing gold_product_performance mart...")

    # Attach English category from dim_products
    items_with_cat = fact_items_df.merge(
        dim_products_df[["product_id", "product_category_name_english"]],
        on="product_id",
        how="left"
    )

    cat_perf = items_with_cat.groupby("product_category_name_english").agg(
        units_sold=("order_item_id", "count"),
        total_revenue=("price", "sum"),
        unique_sellers=("seller_id", "nunique")
    ).reset_index()

    cat_perf["total_revenue"] = cat_perf["total_revenue"].round(2)
    cat_perf["avg_unit_price"] = (cat_perf["total_revenue"] / cat_perf["units_sold"]).round(2)

    return cat_perf.sort_values("total_revenue", ascending=False).reset_index(drop=True)


def build_delivery_kpis_mart(silver_orders_df: pd.DataFrame, silver_customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes delivery latency, late delivery rates, and regional SLAs.
    """
    logger.info("Computing gold_delivery_kpis mart...")

    # Filter to delivered orders with complete delivery timestamps
    delivered = silver_orders_df[
        (silver_orders_df["order_status"] == "delivered") &
        (silver_orders_df["order_delivered_customer_date"].notna())
    ].copy()

    # Join customer to obtain customer_state
    delivered = delivered.merge(
        silver_customers_df[["customer_id", "customer_state"]],
        on="customer_id",
        how="inner"
    )

    # Calculate delivery duration in days
    purchase_ts = pd.to_datetime(delivered["order_purchase_timestamp"])
    delivered_ts = pd.to_datetime(delivered["order_delivered_customer_date"])
    estimated_ts = pd.to_datetime(delivered["order_estimated_delivery_date"])

    delivered["delivery_duration_days"] = (delivered_ts - purchase_ts).dt.total_seconds() / 86400.0

    # Late delivery condition: actual delivery is strictly later than estimated delivery
    delivered["is_late"] = (delivered_ts > estimated_ts).astype(int)
    delivered["order_month"] = purchase_ts.dt.strftime("%Y-%m")

    # Aggregate by customer_state and order_month
    kpis = delivered.groupby(["customer_state", "order_month"]).agg(
        total_delivered_orders=("order_id", "count"),
        late_orders_count=("is_late", "sum"),
        avg_delivery_duration_days=("delivery_duration_days", "mean")
    ).reset_index()

    kpis["late_delivery_rate_pct"] = (kpis["late_orders_count"] / kpis["total_delivered_orders"] * 100).round(2)
    kpis["avg_delivery_duration_days"] = kpis["avg_delivery_duration_days"].round(2)

    return kpis.sort_values(["customer_state", "order_month"]).reset_index(drop=True)


def save_gold_table(df: pd.DataFrame, table_name: str, config: AppConfig) -> str:
    """Writes a Gold analytical table in Snappy Parquet format."""
    if config.storage.storage_mode.lower() == "s3":
        target_uri = f"{config.get_zone_path('gold', table_name)}/{table_name}.parquet"
        storage_options = {}
        if config.aws_access_key_id and config.aws_secret_access_key:
            storage_options = {
                "key": config.aws_access_key_id,
                "secret": config.aws_secret_access_key,
                "client_kwargs": {"region_name": config.aws_region}
            }

        df.to_parquet(
            target_uri,
            engine="pyarrow",
            compression="snappy",
            index=False,
            storage_options=storage_options
        )
        logger.info(f"✓ Saved Gold Parquet to S3: {target_uri}")
        return target_uri
    else:
        target_dir = Path(config.get_zone_path("gold", table_name))
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{table_name}.parquet"

        df.to_parquet(file_path, engine="pyarrow", compression="snappy", index=False)
        logger.info(f"✓ Saved Gold Parquet to local disk: {file_path}")
        return str(file_path)


def run_gold_modeling() -> bool:
    """Orchestrates building all Star Schema tables and Gold KPI marts."""
    config = get_config()
    logger.info("=" * 75)
    logger.info("STARTING GOLD MODELING & ANALYTICS MARTS")
    logger.info("=" * 75)

    silver_dir = Path(config.get_zone_path("silver"))

    # Read Silver Parquet datasets
    silver_orders = pd.read_parquet(silver_dir / "orders")
    silver_items = pd.read_parquet(silver_dir / "order_items")
    silver_payments = pd.read_parquet(silver_dir / "payments")
    silver_customers = pd.read_parquet(silver_dir / "customers")
    silver_products = pd.read_parquet(silver_dir / "products")
    silver_sellers = pd.read_parquet(silver_dir / "sellers")

    # [1] Build Dimensions
    dim_cust = build_dim_customers(silver_customers)
    dim_prod = build_dim_products(silver_products)
    dim_sell = build_dim_sellers(silver_sellers)
    dim_dt = build_dim_date()

    save_gold_table(dim_cust, "dim_customers", config)
    save_gold_table(dim_prod, "dim_products", config)
    save_gold_table(dim_sell, "dim_sellers", config)
    save_gold_table(dim_dt, "dim_date", config)

    # [2] Build Fact Tables
    fact_items = build_fact_order_items(silver_items, silver_orders)
    fact_pay = build_fact_payments(silver_payments, silver_orders)

    save_gold_table(fact_items, "fact_order_items", config)
    save_gold_table(fact_pay, "fact_payments", config)

    # [3] Build Curated Business Marts
    daily_sales = build_daily_sales_mart(fact_items, silver_orders)
    prod_perf = build_product_performance_mart(fact_items, dim_prod)
    delivery_kpis = build_delivery_kpis_mart(silver_orders, silver_customers)

    save_gold_table(daily_sales, "gold_daily_sales", config)
    save_gold_table(prod_perf, "gold_product_performance", config)
    save_gold_table(delivery_kpis, "gold_delivery_kpis", config)

    logger.info("Gold modeling and business marts completed successfully.")
    return True


if __name__ == "__main__":
    run_gold_modeling()
