"""
Unit Tests: Silver Transformations
==================================
Tests individual table cleaning, type conversions, and enrichment functions.
"""

import pandas as pd
import pytest

from transformations.clean_customers import transform_customers
from transformations.clean_orders import transform_orders
from transformations.clean_payments import transform_payments


def test_transform_orders_parsing_and_partitioning():
    """Verify orders transformation extracts year and month partitions."""
    raw = pd.DataFrame({
        "order_id": ["ord_1"],
        "customer_id": ["cust_1"],
        "order_status": ["DELIVERED"],
        "order_purchase_timestamp": ["2018-05-15 14:30:00"],
        "order_approved_at": ["2018-05-15 15:00:00"],
        "order_delivered_carrier_date": ["2018-05-17 10:00:00"],
        "order_delivered_customer_date": ["2018-05-20 18:00:00"],
        "order_estimated_delivery_date": ["2018-05-25 00:00:00"]
    })
    clean_df, bad_df = transform_orders(raw)

    assert len(clean_df) == 1
    assert len(bad_df) == 0
    assert clean_df["order_status"].iloc[0] == "delivered"
    assert clean_df["year"].iloc[0] == "2018"
    assert clean_df["month"].iloc[0] == "05"


def test_transform_customers_state_and_zip():
    """Verify customers normalization standardizes 2-letter uppercase state and 5-digit zip."""
    raw = pd.DataFrame({
        "customer_id": ["c1"],
        "customer_unique_id": ["u1"],
        "customer_zip_code_prefix": ["5704"],  # Needs leading zero
        "customer_city": ["  sao paulo "],
        "customer_state": ["sp"]
    })
    clean_df, bad_df = transform_customers(raw)

    assert len(clean_df) == 1
    assert clean_df["customer_zip_code_prefix"].iloc[0] == "05704"
    assert clean_df["customer_state"].iloc[0] == "SP"
    assert clean_df["customer_city"].iloc[0] == "Sao Paulo"


def test_transform_payments_numeric_casting():
    """Verify payments normalization casts sequential/installments to int and value to float."""
    raw = pd.DataFrame({
        "order_id": ["o1"],
        "payment_sequential": ["1"],
        "payment_type": ["CREDIT_CARD"],
        "payment_installments": ["3"],
        "payment_value": ["150.75"]
    })
    clean_df, bad_df = transform_payments(raw)

    assert len(clean_df) == 1
    assert clean_df["payment_sequential"].iloc[0] == 1
    assert clean_df["payment_installments"].iloc[0] == 3
    assert clean_df["payment_value"].iloc[0] == 150.75
    assert clean_df["payment_type"].iloc[0] == "credit_card"
