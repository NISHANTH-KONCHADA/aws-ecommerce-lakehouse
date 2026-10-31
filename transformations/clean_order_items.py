"""
Silver Transformation: Order Items Dataset
==========================================
Standardizes item-level order records, validating prices, freight, and item sequences.

Transformations Applied:
1. Declared Composite Primary Key: Deduplicates on (order_id, order_item_id).
2. Data Type Enforcement: Casts order_item_id to integer; price and freight_value to double.
3. Domain Validity: Ensures price >= 0.0 and freight_value >= 0.0.
4. Timestamp Parsing: Converts shipping_limit_date to UTC timestamp.
5. Quarantine Routing: Negative prices, negative freight, or duplicate item sequences
   are isolated to the Quarantine zone.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_non_negative, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_order_items(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and standardizes order_items DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Order Items: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Composite Key (order_id, order_item_id)
    df, bad_duplicates = check_uniqueness(df, ["order_id", "order_item_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Cast Types & Validate Non-Negative Numeric Bounds
    df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors="coerce").fillna(1).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")

    df, bad_negatives = check_non_negative(df, ["price", "freight_value"])
    if not bad_negatives.empty:
        quarantine_batches.append(bad_negatives)

    # [Step 4] Parse Shipping Deadline Timestamp
    if "shipping_limit_date" in df.columns:
        df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("order_items", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
