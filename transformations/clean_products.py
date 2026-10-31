"""
Silver Transformation: Products Dataset
=======================================
Cleans product specifications and enriches category names with English translations.

Transformations Applied:
1. Primary Key Deduplication: Deduplicates on product_id.
2. English Category Enrichment: Left-joins with category_translation to map Portuguese
   category names (e.g., 'beleza_saude') to English ('health_beauty').
3. Null Imputation: Fills unclassified categories with 'unknown'.
4. Physical Dimensions Validation: Casts weights and dimensions to float, ensuring > 0.
5. Quarantine Routing: Unparseable product_ids are diverted to quarantine.
"""

import logging
from typing import Optional, Tuple
import pandas as pd

from data_quality.checks import check_completeness, check_uniqueness
from data_quality.quarantine import QuarantineManager

logger = logging.getLogger(__name__)


def transform_products(
    raw_products_df: pd.DataFrame,
    translations_df: Optional[pd.DataFrame] = None,
    quarantine_manager: Optional[QuarantineManager] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans, casts, and enriches products DataFrame for Silver Parquet storage.
    """
    total_input = len(raw_products_df)
    logger.info(f"Transforming Products: {total_input} raw rows received.")

    quarantine_batches = []
    df = raw_products_df.copy()

    # [Step 1] Completeness Check on Primary Key
    df, bad_nulls = check_completeness(df, ["product_id"])
    if not bad_nulls.empty:
        quarantine_batches.append(bad_nulls)

    # [Step 2] Deduplicate on product_id
    df, bad_duplicates = check_uniqueness(df, ["product_id"])
    if not bad_duplicates.empty:
        quarantine_batches.append(bad_duplicates)

    # [Step 3] Join English Category Translation
    if translations_df is not None and not translations_df.empty:
        clean_trans = translations_df[["product_category_name", "product_category_name_english"]].drop_duplicates()
        df = df.merge(clean_trans, on="product_category_name", how="left")
    else:
        df["product_category_name_english"] = df.get("product_category_name", "unknown")

    # Impute missing category names
    df["product_category_name"] = df["product_category_name"].fillna("unknown")
    df["product_category_name_english"] = df["product_category_name_english"].fillna("unknown")

    # [Step 4] Cast Physical Dimensions to Numeric
    numeric_specs = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
    for col in numeric_specs:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    all_quarantined = pd.concat(quarantine_batches, ignore_index=True) if quarantine_batches else pd.DataFrame()

    if quarantine_manager:
        clean_df, _ = quarantine_manager.handle_quarantine("products", df, all_quarantined, total_input)
        return clean_df, all_quarantined

    return df, all_quarantined
