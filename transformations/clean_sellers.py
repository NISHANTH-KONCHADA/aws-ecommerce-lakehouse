"""
Silver Transformation: Sellers Dataset
======================================
Standardizes merchant identifiers, location states, and postal codes.

Transformations Applied:
1. Primary Key Deduplication: Deduplicates on seller_id.
2. Postal Code Normalization: Formats seller_zip_code_prefix as 5-character string.
3. State Code Standardization: Converts seller_state to uppercase 2-letter format.
4. City Formatting: Trims whitespace and title-cases seller_city names.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_sellers(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and standardizes sellers DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Sellers: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Primary Key (seller_id)
    df, bad_duplicates = check_uniqueness(df, ["seller_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Standardize 5-digit Zip Codes
    df["seller_zip_code_prefix"] = (
        df["seller_zip_code_prefix"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("00000")
        .str.zfill(5)
    )

    # [Step 4] Clean State Codes & City
    df["seller_state"] = df["seller_state"].astype(str).str.strip().str.upper()
    df["seller_city"] = df["seller_city"].astype(str).str.strip().str.title()

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("sellers", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
