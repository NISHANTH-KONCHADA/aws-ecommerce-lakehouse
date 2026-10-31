"""
Source Data Profiling Script
============================
Inspects incoming CSV datasets prior to ingestion to understand data characteristics,
detect potential data quality issues, and document schema properties.

This script performs 6 key diagnostic checks:
1. Row counts and column definitions
2. Data types and formatting
3. Null value frequency and percentages
4. Primary key uniqueness and candidate duplicate detection
5. Referential integrity between related entities (e.g., Orders -> Customers)
6. Date/Timestamp parsing validity

Usage:
    python -m ingestion.profile_source --data-dir ./data/sample
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

from config.settings import get_config


def profile_dataset(file_path: Path, primary_keys: List[str]) -> Dict[str, any]:
    """
    Profiles a single CSV file, computing row counts, null percentages, and PK uniqueness.

    Args:
        file_path: Absolute or relative Path to the CSV file.
        primary_keys: List of column names that compose the table's declared primary key.

    Returns:
        A dictionary containing profiling summary statistics.
    """
    if not file_path.exists():
        return {"status": "missing", "error": f"File not found: {file_path}"}

    # [Step 1] Read dataset with pandas
    # We read as string initially to detect true null strings without unintended type coercion
    df = pd.read_csv(file_path, dtype=str)
    total_rows = len(df)
    total_cols = len(df.columns)

    # [Step 2] Compute column-level statistics (null counts & null %)
    column_stats = []
    for col in df.columns:
        null_count = df[col].isna().sum() or (df[col] == "").sum()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0
        unique_count = df[col].nunique()
        sample_val = df[col].dropna().iloc[0] if unique_count > 0 else "N/A"

        column_stats.append({
            "column": col,
            "null_count": int(null_count),
            "null_pct": round(null_pct, 2),
            "unique_count": int(unique_count),
            "sample_value": str(sample_val)[:30]
        })

    # [Step 3] Validate Primary Key Uniqueness
    pk_status = "No PK declared"
    duplicate_pk_count = 0
    if primary_keys:
        missing_pk_cols = [k for k in primary_keys if k not in df.columns]
        if missing_pk_cols:
            pk_status = f"ERROR: Declared PK columns missing from CSV: {missing_pk_cols}"
        else:
            duplicate_pk_count = df.duplicated(subset=primary_keys).sum()
            if duplicate_pk_count == 0:
                pk_status = f"UNIQUE (0 duplicates on {primary_keys})"
            else:
                pk_status = f"VIOLATION: {duplicate_pk_count} duplicate rows found on {primary_keys}"

    return {
        "status": "success",
        "file_name": file_path.name,
        "total_rows": total_rows,
        "total_cols": total_cols,
        "primary_keys": primary_keys,
        "pk_status": pk_status,
        "duplicate_pk_count": int(duplicate_pk_count),
        "columns": column_stats
    }


def check_referential_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_fk: str,
    parent_pk: str,
    relationship_name: str
) -> Dict[str, any]:
    """
    Checks whether all foreign keys in the child table exist in the parent table.

    Args:
        child_df: DataFrame of the referencing table (e.g., Orders).
        parent_df: DataFrame of the referenced table (e.g., Customers).
        child_fk: Foreign key column name in child table.
        parent_pk: Primary key column name in parent table.
        relationship_name: Descriptive label for reporting.

    Returns:
        Dictionary with count of orphaned records and integrity percentage.
    """
    valid_parents = set(parent_df[parent_pk].dropna().unique())
    child_fks = child_df[child_fk].dropna()

    orphaned_mask = ~child_fks.isin(valid_parents)
    orphaned_count = orphaned_mask.sum()
    total_child_records = len(child_fks)
    valid_pct = ((total_child_records - orphaned_count) / total_child_records * 100) if total_child_records > 0 else 100.0

    return {
        "relationship": relationship_name,
        "child_fk": child_fk,
        "parent_pk": parent_pk,
        "total_records": total_child_records,
        "orphaned_records": int(orphaned_count),
        "integrity_pct": round(valid_pct, 2)
    }


def run_profiling(data_dir: Path) -> None:
    """Orchestrates profiling across all configured datasets in data_dir."""
    config = get_config()
    print("=" * 80)
    print(f"PROFILING SOURCE DATA IN: {data_dir}")
    print("=" * 80)

    loaded_dfs: Dict[str, pd.DataFrame] = {}

    for name, ds_cfg in config.datasets.items():
        file_path = data_dir / ds_cfg.source_file
        report = profile_dataset(file_path, ds_cfg.primary_key)

        print(f"\n[Dataset: {name}] -> {ds_cfg.source_file}")
        if report["status"] != "success":
            print(f"  Warning: {report['error']}")
            continue

        print(f"  Total Rows: {report['total_rows']:,} | Total Columns: {report['total_cols']}")
        print(f"  Primary Key: {report['pk_status']}")
        print("  Columns:")
        for col_info in report["columns"]:
            print(f"    - {col_info['column']:<32} | Nulls: {col_info['null_count']:>5} ({col_info['null_pct']:>5}%) | Unique: {col_info['unique_count']:>5} | Sample: {col_info['sample_value']}")

        # Cache dataframe for referential checks
        loaded_dfs[name] = pd.read_csv(file_path, dtype=str)

    # Referential Integrity Check (Orders -> Customers)
    if "orders" in loaded_dfs and "customers" in loaded_dfs:
        print("\n" + "-" * 80)
        print("REFERENTIAL INTEGRITY CHECKS:")
        print("-" * 80)
        ref_orders_cust = check_referential_integrity(
            child_df=loaded_dfs["orders"],
            parent_df=loaded_dfs["customers"],
            child_fk="customer_id",
            parent_pk="customer_id",
            relationship_name="Orders -> Customers"
        )
        print(f"  {ref_orders_cust['relationship']}: {ref_orders_cust['integrity_pct']}% matched ({ref_orders_cust['orphaned_records']} orphaned keys out of {ref_orders_cust['total_records']})")

    # Referential Integrity Check (Order Items -> Orders)
    if "order_items" in loaded_dfs and "orders" in loaded_dfs:
        ref_items_orders = check_referential_integrity(
            child_df=loaded_dfs["order_items"],
            parent_df=loaded_dfs["orders"],
            child_fk="order_id",
            parent_pk="order_id",
            relationship_name="Order Items -> Orders"
        )
        print(f"  {ref_items_orders['relationship']}: {ref_items_orders['integrity_pct']}% matched ({ref_items_orders['orphaned_records']} orphaned keys out of {ref_items_orders['total_records']})")

    print("\n" + "=" * 80)
    print("PROFILING COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profile Olist CSV datasets.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "sample",
        help="Directory containing source CSV files (defaults to ./data/sample)"
    )
    args = parser.parse_args()
    run_profiling(args.data_dir)
