"""
Silver Transformation: Reviews Dataset
======================================
Validates review scores, parses survey timestamps, and handles missing comments.

Transformations Applied:
1. Primary Key Deduplication: Deduplicates on review_id.
2. Review Score Range Check: Enforces satisfaction ratings between 1 and 5 inclusive.
3. Timestamp Parsing: Converts review_creation_date and review_answer_timestamp to UTC.
4. Text Handling: Preserves missing comment titles and messages as empty strings without
   discarding the valid numeric score.
5. Quarantine Routing: Out-of-range review scores (e.g. 0, 6, -1) are quarantined.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_uniqueness, check_value_range
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_reviews(
    raw_df: pd.DataFrame,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and standardizes reviews DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_df)
    logger.info(f"Transforming Reviews: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_df.copy()

    # [Step 1] Completeness Check on Mandatory Fields
    mandatory = ["review_id", "order_id", "review_score"]
    df, bad_nulls = check_completeness(df, mandatory)
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on Primary Key (review_id)
    df, bad_duplicates = check_uniqueness(df, ["review_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Cast Score & Validate Range [1, 5]
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
    df, bad_scores = check_value_range(df, column="review_score", min_val=1, max_val=5)
    if not bad_scores.empty:
        quarantine_batches.append(bad_scores)

    # Convert review_score to integer
    df["review_score"] = df["review_score"].astype(int)

    # [Step 4] Parse Timestamps
    for col in ["review_creation_date", "review_answer_timestamp"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # [Step 5] Handle Missing Comment Text without Dropping Rows
    df["review_comment_title"] = df["review_comment_title"].fillna("").astype(str).str.strip()
    df["review_comment_message"] = df["review_comment_message"].fillna("").astype(str).str.strip()

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("reviews", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
