"""
Unit Tests: Data Quality Checks
===============================
Validates individual data quality rules and quarantine categorization logic.
"""

import pandas as pd
import pytest

from data_quality.checks import (
    check_chronology,
    check_completeness,
    check_non_negative,
    check_uniqueness,
    check_value_range,
)


def test_check_completeness_identifies_nulls():
    """Verify that rows with null or empty mandatory fields are separated."""
    df = pd.DataFrame({
        "order_id": ["o1", "o2", None, "o4"],
        "status": ["delivered", "", "shipped", "delivered"]
    })
    valid_df, invalid_df = check_completeness(df, ["order_id", "status"])

    assert len(valid_df) == 2
    assert len(invalid_df) == 2
    assert "MISSING_MANDATORY_FIELDS" in invalid_df["_quarantine_reason"].iloc[0]


def test_check_uniqueness_deduplicates_primary_keys():
    """Verify that duplicate primary keys are quarantined while keeping the first occurrence."""
    df = pd.DataFrame({
        "id": ["k1", "k2", "k1", "k3"],
        "value": [10, 20, 30, 40]
    })
    valid_df, invalid_df = check_uniqueness(df, ["id"])

    assert len(valid_df) == 3
    assert len(invalid_df) == 1
    assert invalid_df["value"].iloc[0] == 30
    assert "DUPLICATE_PRIMARY_KEY" in invalid_df["_quarantine_reason"].iloc[0]


def test_check_non_negative_isolates_negative_values():
    """Verify that negative financial amounts are flagged."""
    df = pd.DataFrame({
        "price": [10.5, -5.0, 0.0, 99.9],
        "freight": [2.0, 3.0, -1.0, 5.0]
    })
    valid_df, invalid_df = check_non_negative(df, ["price", "freight"])

    assert len(valid_df) == 2  # Only row 0 and 3 are non-negative on both
    assert len(invalid_df) == 2


def test_check_value_range():
    """Verify that scores outside [min_val, max_val] are flagged."""
    df = pd.DataFrame({"score": [1, 5, 3, 0, 6, -1]})
    valid_df, invalid_df = check_value_range(df, "score", min_val=1, max_val=5)

    assert len(valid_df) == 3
    assert len(invalid_df) == 3


def test_check_chronology():
    """Verify that rows with end timestamp before start timestamp are quarantined."""
    df = pd.DataFrame({
        "order_ts": ["2018-01-01 10:00:00", "2018-01-05 10:00:00"],
        "delivered_ts": ["2018-01-07 10:00:00", "2018-01-02 10:00:00"]  # Row 1 is inverted
    })
    valid_df, invalid_df = check_chronology(df, "order_ts", "delivered_ts")

    assert len(valid_df) == 1
    assert len(invalid_df) == 1
    assert "CHRONOLOGICAL_INVERSION" in invalid_df["_quarantine_reason"].iloc[0]
