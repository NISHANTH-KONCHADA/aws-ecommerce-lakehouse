"""
Dimensional Modeling: Fact Tables
=================================
Constructs core analytical fact tables adhering strictly to declared granularities.

Fact Tables Created:
1. fact_order_items:
   - Declared Grain: One row per line item inside an order (order_id + order_item_id).
   - Foreign Keys: product_id, seller_id, customer_id, order_date_key.
   - Measures: price, freight_value, total_item_value (price + freight).
2. fact_payments:
   - Declared Grain: One row per payment transaction (order_id + payment_sequential).
   - Foreign Keys: customer_id, order_date_key.
   - Measures: payment_value, payment_installments.

Why We Keep These Fact Tables Separate:
Joining items and payments directly at row-level causes an N x M Cartesian product
(fan-out), producing inflated revenue and payment figures. Keeping them as separate
fact tables connected via conformed dimensions preserves mathematical precision.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def build_fact_order_items(
    silver_items_df: pd.DataFrame,
    silver_orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Constructs fact_order_items by joining item lines with order metadata.
    """
    logger.info("Building fact_order_items...")

    # Select relevant order attributes for context
    orders_subset = silver_orders_df[[
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    ]].copy()

    # Join items with orders to attach customer_id and order_purchase_timestamp
    fact = silver_items_df.merge(orders_subset, on="order_id", how="inner")

    # Generate date_key FK pointing to dim_date (YYYYMMDD)
    fact["order_date_key"] = pd.to_datetime(fact["order_purchase_timestamp"]).dt.strftime("%Y%m%d").astype(int)

    # Calculate item-level total gross
    fact["total_item_value"] = (fact["price"] + fact["freight_value"]).round(2)

    # Reorder columns logically
    columns_order = [
        "order_id",
        "order_item_id",
        "order_date_key",
        "customer_id",
        "product_id",
        "seller_id",
        "order_status",
        "shipping_limit_date",
        "price",
        "freight_value",
        "total_item_value"
    ]
    return fact[columns_order]


def build_fact_payments(
    silver_payments_df: pd.DataFrame,
    silver_orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Constructs fact_payments by joining payment installments with order metadata.
    """
    logger.info("Building fact_payments...")

    orders_subset = silver_orders_df[[
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    ]].copy()

    fact = silver_payments_df.merge(orders_subset, on="order_id", how="inner")
    fact["order_date_key"] = pd.to_datetime(fact["order_purchase_timestamp"]).dt.strftime("%Y%m%d").astype(int)

    columns_order = [
        "order_id",
        "payment_sequential",
        "order_date_key",
        "customer_id",
        "order_status",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]
    return fact[columns_order]
