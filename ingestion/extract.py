"""
Unified Ingestion Pipeline Runner
=================================
Orchestrates the extraction, pre-landing validation, Bronze zone upload,
and manifest logging for all e-commerce datasets.

Execution Flow:
1. Load configuration and dataset definitions from config/config.yaml.
2. Initialize IngestionManifest session.
3. For each configured dataset:
   a. Check file existence, non-emptiness, and mandatory column headers.
   b. If valid, ingest to Bronze zone (S3 prefix or local bronze directory) partitioned by ingestion_date.
   c. Record file checksum (SHA-256), row count, byte size, and status in manifest.
4. Save manifest audit log to S3 / local manifests directory.
5. Display summary report.

Usage:
    # Run against sample data (Local or S3 depending on .env)
    python -m ingestion.extract --source-dir ./data/sample

    # Run against full downloaded Kaggle datasets
    python -m ingestion.extract --source-dir ./data/raw
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config.settings import AppConfig, get_config
from ingestion.manifest import IngestionManifest
from ingestion.upload_to_s3 import BronzeIngestionService
from ingestion.validate_source import validate_file_structure

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestion.extract")


def run_ingestion(source_dir: Path, ingestion_date: Optional[str] = None) -> bool:
    """
    Executes the ingestion pipeline for all configured datasets.

    Args:
        source_dir: Path containing the input CSV files.
        ingestion_date: Date partition string YYYY-MM-DD (defaults to UTC today).

    Returns:
        True if all files were ingested successfully, False otherwise.
    """
    if not ingestion_date:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    config: AppConfig = get_config()
    manifest = IngestionManifest(config)
    ingestion_service = BronzeIngestionService(config)

    logger.info("=" * 75)
    logger.info(f"STARTING INGESTION TO BRONZE LAYER")
    logger.info(f"Source Directory: {source_dir}")
    logger.info(f"Ingestion Date Partition: {ingestion_date}")
    logger.info(f"Storage Mode: {config.storage.storage_mode.upper()}")
    logger.info(f"Run ID: {manifest.run_id}")
    logger.info("=" * 75)

    all_success = True

    for dataset_name, ds_cfg in config.datasets.items():
        file_path = source_dir / ds_cfg.source_file
        logger.info(f"\nProcessing [{dataset_name}] -> {file_path.name}")

        # [Step 1] Validate file structure and required headers
        is_valid, validation_msg, metadata = validate_file_structure(
            file_path=file_path,
            mandatory_columns=ds_cfg.mandatory_columns
        )

        if not is_valid:
            logger.error(f"✕ Skipping {dataset_name}: {validation_msg}")
            manifest.record_file(
                dataset_name=dataset_name,
                source_path=str(file_path),
                target_uri="",
                sha256="",
                size_bytes=metadata.get("byte_size", 0),
                row_count=0,
                status="VALIDATION_FAILED",
                error_message=validation_msg
            )
            all_success = False
            continue

        # [Step 2] Ingest to Bronze zone
        success, target_uri, upload_details = ingestion_service.ingest_file(
            source_file_path=file_path,
            dataset_name=dataset_name,
            ingestion_date=ingestion_date
        )

        if success:
            manifest.record_file(
                dataset_name=dataset_name,
                source_path=str(file_path),
                target_uri=target_uri,
                sha256=upload_details["sha256"],
                size_bytes=upload_details["size_bytes"],
                row_count=metadata["row_count"],
                status="SUCCESS"
            )
        else:
            manifest.record_file(
                dataset_name=dataset_name,
                source_path=str(file_path),
                target_uri="",
                sha256=upload_details.get("sha256", ""),
                size_bytes=upload_details.get("size_bytes", 0),
                row_count=metadata["row_count"],
                status="UPLOAD_FAILED",
                error_message=upload_details.get("error")
            )
            all_success = False

    # [Step 3] Finalize & Persist Manifest
    manifest_location = manifest.save_manifest()

    logger.info("\n" + "=" * 75)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 75)
    logger.info(f"Run ID:            {manifest.run_id}")
    logger.info(f"Total Processed:   {len(manifest.records)}")
    logger.info(f"Successful:        {sum(1 for r in manifest.records if r['status'] == 'SUCCESS')}")
    logger.info(f"Failed:            {sum(1 for r in manifest.records if r['status'] != 'SUCCESS')}")
    logger.info(f"Manifest Location: {manifest_location}")
    logger.info("=" * 75)

    return all_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV datasets into Bronze zone.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "sample",
        help="Directory containing source CSV files (defaults to ./data/sample)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Ingestion date partition (YYYY-MM-DD). Defaults to UTC today."
    )
    args = parser.parse_args()
    success = run_ingestion(args.source_dir, args.date)
    sys.exit(0 if success else 1)

