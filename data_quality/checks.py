"""
Data Quality Validation Rules Module
====================================
Contains reusable, deterministic validation checks applied during Silver transformation.

Each check function inspects a pandas/Spark DataFrame and identifies:
1. Conforming records (passed all rules)
2. Non-conforming records (failed a rule, tagged with a descriptive reason code)

Key Rule Categories:
- Completeness: Mandatory columns must not be null or blank.
- Uniqueness: Primary key composite combinations must not have duplicate rows.
- Domain Validity: Numeric metrics (price, payment_value, freight) must be non-negative.
- Range Validity: Review scores must be integers between 1 and 5.
- Chronological Order: Delivered date must be after purchase date.

Why This Matters in Production:
Silent data corruption (e.g., negative prices or future delivery dates) ruins downstream
BI dashboards. Tagging and isolating bad data into a Quarantine zone guarantees data lake
integrity without crashing the entire batch pipeline.
"""

from typing import List, Optional, Set, Tuple
import pandas as pd


def check_completeness(df: pd.DataFrame, mandatory_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates that declared mandatory columns are non-null and non-empty.

    Args:
        df: Input DataFrame.
        mandatory_columns: Columns that must contain valid values.

    Returns:
        Tuple of (valid_df, invalid_df). Invalid rows contain column '_quarantine_reason'.
    """
    if df.empty:
        return df.copy(), pd.DataFrame()

    null_conditions = []
    for col in mandatory_columns:
        if col in df.columns:
            # Check for NaN, None, or empty string whitespace
            is_null = df[col].isna() | (df[col].astype(str).str.strip() == "")
            null_conditions.append(is_null)

    if not null_conditions:
        return df.copy(), pd.DataFrame()

    # Combine conditions: True if any mandatory column is null
    any_null = pd.concat(null_conditions, axis=1).any(axis=1)

    valid_df = df[~any_null].copy()
    invalid_df = df[any_null].copy()

    if not invalid_df.empty:
        # Construct helpful reason string indicating exactly which column was null
        reasons = []
        for idx in invalid_df.index:
            missing_cols = [
                col for col in mandatory_columns
                if col in df.columns and (pd.isna(df.loc[idx, col]) or str(df.loc[idx, col]).strip() == "")
            ]
            reasons.append(f"MISSING_MANDATORY_FIELDS: {', '.join(missing_cols)}")
        invalid_df["_quarantine_reason"] = reasons

    return valid_df, invalid_df


def check_uniqueness(df: pd.DataFrame, primary_key: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates that declared primary keys are unique. Keeps the first occurrence and
    routes duplicate records to quarantine.

    Args:
        df: Input DataFrame.
        primary_key: Columns composing the unique primary key.

    Returns:
        Tuple of (deduplicated_valid_df, duplicate_invalid_df).
    """
    if df.empty or not primary_key:
        return df.copy(), pd.DataFrame()

    # Identify duplicate rows (keep='first' marks later occurrences as True)
    is_duplicate = df.duplicated(subset=primary_key, keep="first")

    valid_df = df[~is_duplicate].copy()
    invalid_df = df[is_duplicate].copy()

    if not invalid_df.empty:
        invalid_df["_quarantine_reason"] = f"DUPLICATE_PRIMARY_KEY: {primary_key}"

    return valid_df, invalid_df


def check_non_negative(df: pd.DataFrame, numeric_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates that specified numeric columns are strictly non-negative (>= 0.0).

    Args:
        df: Input DataFrame.
        numeric_columns: List of numeric columns to evaluate.

    Returns:
        Tuple of (valid_df, invalid_df).
    """
    if df.empty or not numeric_columns:
        return df.copy(), pd.DataFrame()

    negative_conditions = []
    for col in numeric_columns:
        if col in df.columns:
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            is_neg = numeric_series < 0.0
            negative_conditions.append(is_neg)

    if not negative_conditions:
        return df.copy(), pd.DataFrame()

    any_negative = pd.concat(negative_conditions, axis=1).any(axis=1)

    valid_df = df[~any_negative].copy()
    invalid_df = df[any_negative].copy()

    if not invalid_df.empty:
        invalid_df["_quarantine_reason"] = "NEGATIVE_NUMERIC_VALUE"

    return valid_df, invalid_df


def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_val: float,
    max_val: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates that a numeric column falls within [min_val, max_val] inclusive.
    E.g., review scores must be in range 1 to 5.
    """
    if df.empty or column not in df.columns:
        return df.copy(), pd.DataFrame()

    numeric_series = pd.to_numeric(df[column], errors="coerce")
    out_of_bounds = (numeric_series < min_val) | (numeric_series > max_val)

    valid_df = df[~out_of_bounds].copy()
    invalid_df = df[out_of_bounds].copy()

    if not invalid_df.empty:
        invalid_df["_quarantine_reason"] = f"OUT_OF_BOUNDS_VALUE: {column} not in [{min_val}, {max_val}]"

    return valid_df, invalid_df


def check_chronology(
    df: pd.DataFrame,
    start_col: str,
    end_col: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates chronological sequence: end_col must be >= start_col when both are present.
    Example: order_delivered_customer_date >= order_purchase_timestamp.
    """
    if df.empty or start_col not in df.columns or end_col not in df.columns:
        return df.copy(), pd.DataFrame()

    start_dt = pd.to_datetime(df[start_col], errors="coerce")
    end_dt = pd.to_datetime(df[end_col], errors="coerce")

    # Only evaluate rows where BOTH timestamps are populated
    both_present = start_dt.notna() & end_dt.notna()
    is_inverted = both_present & (end_dt < start_dt)

    valid_df = df[~is_inverted].copy()
    invalid_df = df[is_inverted].copy()

    if not invalid_df.empty:
        invalid_df["_quarantine_reason"] = f"CHRONOLOGICAL_INVERSION: {end_col} before {start_col}"

    return valid_df, invalid_df
