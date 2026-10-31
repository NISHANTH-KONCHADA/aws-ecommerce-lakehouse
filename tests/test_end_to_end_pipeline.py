"""
Integration Test: End-to-End Pipeline
=====================================
Executes the full local lifecycle:
1. Ingestion: Reads sample CSVs -> writes Bronze with manifests
2. Silver: Reads Bronze -> validates -> writes partitioned Parquet
3. Gold: Reads Silver -> models facts & dims -> creates KPI marts

Verifies that all layers run sequentially without exceptions and generate
valid Snappy Parquet tables.
"""

from pathlib import Path
import pandas as pd
import pytest

from config.settings import get_config
from ingestion.extract import run_ingestion
from modeling.build_gold_marts import run_gold_modeling
from transformations.runner import run_silver_transformations


def test_end_to_end_pipeline_execution():
    """Validates complete Bronze -> Silver -> Gold execution."""
    config = get_config()
    sample_dir = Path(__file__).resolve().parent.parent / "data" / "sample"

    # [1] Ingestion
    ingest_success = run_ingestion(source_dir=sample_dir, ingestion_date="2026-10-31")
    assert ingest_success is True

    # [2] Silver Transformations
    silver_success = run_silver_transformations()
    assert silver_success is True

    # [3] Gold Modeling
    gold_success = run_gold_modeling()
    assert gold_success is True

    # [4] Assert Gold Output Tables
    gold_dir = Path(config.get_zone_path("gold"))
    assert (gold_dir / "dim_customers" / "dim_customers.parquet").exists()
    assert (gold_dir / "fact_order_items" / "fact_order_items.parquet").exists()
    assert (gold_dir / "gold_daily_sales" / "gold_daily_sales.parquet").exists()
    assert (gold_dir / "gold_delivery_kpis" / "gold_delivery_kpis.parquet").exists()

    # [5] Verify row counts > 0
    daily_sales = pd.read_parquet(gold_dir / "gold_daily_sales" / "gold_daily_sales.parquet")
    assert len(daily_sales) > 0
    assert "gross_revenue" in daily_sales.columns
    assert "average_order_value" in daily_sales.columns
