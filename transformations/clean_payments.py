"""
Silver Transformation: Payments Dataset
=======================================
Cleans and validates customer payment records, installment counts, and transaction amounts.

Transformations Applied:
1. Declared Composite Primary Key: Deduplicates on (order_id, payment_sequential).
2. Data Type Enforcement: Casts payment_sequential and payment_installments to int,
   and payment_value to double.
3. Domain Validity: Ensures payment_value >= 0.0 and payment_installments >= 1.
4. Payment Method Normalization: Trims and lowercases payment_type strings.
5. Quarantine Routing: Negative payment amounts or corrupted sequences are routed to quarantine.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_non_negative, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_payments(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and standardizes payments DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Payments: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Composite Key (order_id, payment_sequential)
    df, bad_duplicates = check_uniqueness(df, ["order_id", "payment_sequential"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Cast Types & Validate Non-Negative Values
    df["payment_sequential"] = pd.to_numeric(df["payment_sequential"], errors="coerce").fillna(1).astype(int)
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce").fillna(1).astype(int)
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")

    df, bad_negatives = check_non_negative(df, ["payment_value"])
    if not bad_negatives.empty:
        quarantine_batches.append(bad_negatives)

    # [Step 4] Validate Installments >= 1
    bad_installments = df[df["payment_installments"] < 1].copy()
    if not bad_installments.empty:
        bad_installments["_quarantine_reason"] = "INVALID_INSTALLMENTS_LESS_THAN_ONE"
        quarantine_batches.append(bad_installments)
        df = df[df["payment_installments"] >= 1].copy()

    # [Step 5] Normalize payment_type
    df["payment_type"] = df["payment_type"].astype(str).str.strip().str.lower()

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("payments", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
