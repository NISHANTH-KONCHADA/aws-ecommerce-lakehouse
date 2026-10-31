"""
Dimensional Modeling: Conformed Dimensions
==========================================
Constructs reusable dimension tables for the Kimball-style star schema in the Gold layer.

Dimensions Created:
1. dim_customers:
   - Grain: One row per customer_id.
   - Attributes: customer_id, customer_unique_id (tracks repeat purchasers),
     customer_city, customer_state, customer_zip_code_prefix.
2. dim_products:
   - Grain: One row per product_id.
   - Attributes: product_id, product_category_name, product_category_name_english,
     weight, dimensions, photo counts.
3. dim_sellers:
   - Grain: One row per seller_id.
   - Attributes: seller_id, seller_city, seller_state, seller_zip_code_prefix.
4. dim_date:
   - Grain: One row per calendar day across the entire e-commerce operating span.
   - Attributes: date_key (YYYYMMDD integer), full_date, year, month, month_name,
     day, quarter, day_of_week, day_name, is_weekend.

Why a Conformed Date Dimension Matters:
Allows BI analysts to slice metrics by fiscal quarters, weekends, and day-of-week trends
without having to parse expensive date functions in every Athena SQL query.
"""

import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def build_dim_customers(silver_customers_df: pd.DataFrame) -> pd.DataFrame:
    """Constructs dim_customers from Silver customers."""
    logger.info("Building dim_customers...")
    dim = silver_customers_df[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix"
    ]].drop_duplicates(subset=["customer_id"]).copy()
    return dim


def build_dim_products(silver_products_df: pd.DataFrame) -> pd.DataFrame:
    """Constructs dim_products from Silver products."""
    logger.info("Building dim_products...")
    dim = silver_products_df[[
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]].drop_duplicates(subset=["product_id"]).copy()
    return dim


def build_dim_sellers(silver_sellers_df: pd.DataFrame) -> pd.DataFrame:
    """Constructs dim_sellers from Silver sellers."""
    logger.info("Building dim_sellers...")
    dim = silver_sellers_df[[
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix"
    ]].drop_duplicates(subset=["seller_id"]).copy()
    return dim


def build_dim_date(start_date: str = "2016-01-01", end_date: str = "2019-12-31") -> pd.DataFrame:
    """
    Generates a full calendar date dimension table.

    Args:
        start_date: Start of calendar range (YYYY-MM-DD).
        end_date: End of calendar range (YYYY-MM-DD).

    Returns:
        DataFrame representing dim_date.
    """
    logger.info(f"Generating dim_date from {start_date} to {end_date}...")
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["quarter"] = df["full_date"].dt.quarter
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.strftime("%B")
    df["day"] = df["full_date"].dt.day
    df["day_of_week"] = df["full_date"].dt.dayofweek + 1  # 1 = Monday, 7 = Sunday
    df["day_name"] = df["full_date"].dt.strftime("%A")
    df["is_weekend"] = df["day_of_week"].isin([6, 7]).astype(int)

    return df
