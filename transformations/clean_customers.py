"""
Silver Transformation: Customers Dataset
========================================
Standardizes customer records, postal code prefixes, and Brazilian state abbreviations.

Transformations Applied:
1. Primary Key Uniqueness: Deduplicates on customer_id.
2. Postal Code Normalization: Formats customer_zip_code_prefix as 5-character string
   with leading zeros (e.g., '5704' -> '05704') to match Brazilian CEP standards.
3. State Code Standardization: Converts customer_state to uppercase 2-letter format.
4. City Formatting: Trims whitespace and title-cases customer_city names.
5. Quarantine Routing: Records with missing customer_id or malformed states are quarantined.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_customers(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and standardizes customers DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Customers: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Primary Key (customer_id)
    df, bad_duplicates = check_uniqueness(df, ["customer_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Standardize 5-digit Brazilian Zip Codes (CEP prefix)
    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("00000")
        .str.zfill(5)
    )

    # [Step 4] Clean State Codes (Uppercase 2-letters) & City (Title Case)
    df["customer_state"] = df["customer_state"].astype(str).str.strip().str.upper()
    df["customer_city"] = df["customer_city"].astype(str).str.strip().str.title()

    # Quarantine any record with an invalid state code length
    bad_states = df[df["customer_state"].str.len() != 2].copy()
    if not bad_states.empty:
        bad_states["_quarantine_reason"] = "INVALID_STATE_CODE_LENGTH"
        quarantine_batches.append(bad_states)
        df = df[df["customer_state"].str.len() == 2].copy()

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("customers", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
