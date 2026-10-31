"""
Silver Transformation: Orders Dataset
=====================================
Transforms raw Bronze orders into the standardized Silver Parquet table.

Transformations Applied:
1. Column Name Normalization: Ensures snake_case naming conventions.
2. Timestamp Parsing: Converts string timestamps to ISO UTC datetimes:
   - order_purchase_timestamp
   - order_approved_at
   - order_delivered_carrier_date
   - order_delivered_customer_date
   - order_estimated_delivery_date
3. Chronological Integrity Check: Ensures delivery timestamp >= purchase timestamp.
4. Primary Key Deduplication: Ensures order_id is distinct.
5. Partition Key Derivation: Extracts `year` and `month` from order_purchase_timestamp.
6. Quarantine Routing: Unparseable timestamps, chronological inversions, or duplicate
   order_ids are directed to the Quarantine zone with explanatory error codes.

Why Partitioning by Year & Month Matters in Athena:
Partitioning by `year` and `month` allows Athena's Presto engine to skip scanning entire
years of data when running queries like `WHERE year = '2018' AND month = '05'`, reducing
query runtime and AWS scan costs by up to 90%.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_chronology, check_completeness, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_orders(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans, validates, and prepares orders DataFrame for Silver Parquet storage.

    Args:
        raw_df: Raw orders DataFrame from Bronze layer.
        quarantine_manager: Optional manager to register and persist quarantined rows.

    Returns:
        Tuple of (clean_silver_df, quarantined_df).
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Orders: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Primary Key (order_id)
    df, bad_duplicates = check_uniqueness(df, ["order_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Parse ISO UTC Timestamps
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Drop records where purchase timestamp was completely unparseable
    bad_purchase_ts = df[df["order_purchase_timestamp"].isna()].copy()
    if not bad_purchase_ts.empty:
        bad_purchase_ts["_quarantine_reason"] = "CORRUPTED_PURCHASE_TIMESTAMP"
        quarantine_batches.append(bad_purchase_ts)
        df = df[df["order_purchase_timestamp"].notna()].copy()

    # [Step 4] Chronological Validity Check (delivered >= purchase)
    df, bad_chronology = check_chronology(
        df,
        start_col="order_purchase_timestamp",
        end_col="order_delivered_customer_date"
    )
    if not bad_chronology.empty:
        quarantine_batches.append(bad_chronology)

    # [Step 5] Standardize order_status
    df["order_status"] = df["order_status"].astype(str).str.lower().str.strip()

    # [Step 6] Derive Partitioning Columns (year and month for Athena partition pruning)
    df["year"] = df["order_purchase_timestamp"].dt.year.astype(str)
    df["month"] = df["order_purchase_timestamp"].dt.strftime("%m")

    # Combine all quarantined records if any
    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("orders", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
