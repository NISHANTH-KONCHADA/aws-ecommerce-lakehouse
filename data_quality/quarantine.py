"""
Quarantine Management Module
============================
Handles the isolation, persistence, and reporting of non-conforming data records.

Features:
1. Adds traceability metadata (_quarantine_timestamp, _table_name, _pipeline_run_id).
2. Persists rejected records to S3 or local directory in JSON or Parquet format.
3. Compiles a high-level Data Quality scorecard for every batch execution.

Why Isolating Bad Records Matters:
Data pipelines should never silently drop invalid records. Business operations and audit
teams need to know:
- Exactly how many records were rejected.
- The root cause for rejection (e.g., negative price, duplicate key).
- An accessible location where bad data can be examined, corrected, and re-ingested.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

from config.settings import AppConfig, get_config

logger = logging.getLogger(__name__)


class QuarantineManager:
    """Manages collection, persistence, and audit logging of invalid records."""

    def __init__(self, run_id: str, config: Optional[AppConfig] = None):
        self.run_id = run_id
        self.config = config or get_config()
        self.summary_stats: Dict[str, Dict[str, int]] = {}

    def handle_quarantine(
        self,
        table_name: str,
        valid_df: pd.DataFrame,
        quarantine_df: pd.DataFrame,
        total_input_count: int
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Processes clean records and persists rejected records to the quarantine zone.

        Args:
            table_name: Logical table name (e.g., 'orders', 'payments').
            valid_df: DataFrame of conforming records.
            quarantine_df: DataFrame of non-conforming records with '_quarantine_reason'.
            total_input_count: Total row count before filtering.

        Returns:
            Tuple of (clean_df, quarantine_file_uri_or_None).
        """
        quarantined_count = len(quarantine_df)
        valid_count = len(valid_df)

        self.summary_stats[table_name] = {
            "input_rows": total_input_count,
            "valid_rows": valid_count,
            "quarantined_rows": quarantined_count
        }

        if quarantined_count == 0:
            logger.info(f"✓ [{table_name}] 100% data quality passed ({valid_count}/{total_input_count} rows).")
            return valid_df, None

        logger.warning(
            f"⚠ [{table_name}] {quarantined_count} of {total_input_count} records routed to quarantine."
        )

        # Append audit metadata to quarantined records
        quarantined_records = quarantine_df.copy()
        quarantined_records["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
        quarantined_records["_pipeline_run_id"] = self.run_id
        quarantined_records["_table_name"] = table_name

        # Persist quarantine records
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_quarantine_{timestamp_str}_{self.run_id[:8]}.json"

        if self.config.storage.storage_mode.lower() == "s3":
            # Write to S3
            import boto3
            bucket = self.config.storage.s3_bucket
            s3_key = f"{self.config.storage.zones.get('quarantine', 'quarantine')}/{table_name}/{filename}"
            uri = f"s3://{bucket}/{s3_key}"

            json_payload = quarantined_records.to_json(orient="records", date_format="iso", indent=2)

            session_kwargs = {"region_name": self.config.aws_region}
            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            s3_client = boto3.client("s3", **session_kwargs)
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=json_payload,
                ContentType="application/json",
                ServerSideEncryption="AES256"
            )
            logger.info(f"Quarantined records saved to S3: {uri}")
            return valid_df, uri
        else:
            # Write to local quarantine directory
            quarantine_dir = Path(self.config.get_zone_path("quarantine", table_name))
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            target_path = quarantine_dir / filename

            quarantined_records.to_json(target_path, orient="records", date_format="iso", indent=2)
            logger.info(f"Quarantined records saved locally: {target_path}")
            return valid_df, str(target_path)

    def print_quality_scorecard(self) -> None:
        """Displays a formatted table of all data quality metrics across datasets."""
        print("\n" + "=" * 80)
        print("DATA QUALITY SCORECARD & RECONCILIATION")
        print("=" * 80)
        print(f"{'Table':<22} | {'Input Rows':>10} | {'Valid Rows':>10} | {'Quarantined':>12} | {'Pass Rate':>10}")
        print("-" * 80)

        for table, stats in self.summary_stats.items():
            inp = stats["input_rows"]
            val = stats["valid_rows"]
            qua = stats["quarantined_rows"]
            pass_rate = f"{(val / inp * 100):.1f}%" if inp > 0 else "0.0%"
            print(f"{table:<22} | {inp:>10,d} | {val:>10,d} | {qua:>12,d} | {pass_rate:>10}")

        print("=" * 80 + "\n")
