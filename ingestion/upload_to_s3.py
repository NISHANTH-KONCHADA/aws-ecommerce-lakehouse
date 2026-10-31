"""
Bronze Zone Ingestion & S3 Upload Module
========================================
Transfers validated raw source files to the Bronze layer of the data lake.

Dual Storage Support:
- AWS S3: Uses boto3 to upload files with server-side encryption (SSE-S3/AES256)
  and organizes files by:
  `s3://<bucket>/bronze/<dataset_name>/ingestion_date=YYYY-MM-DD/<file_name>`
- Local Mode: Copies raw files into local directory structure:
  `./data/bronze/<dataset_name>/ingestion_date=YYYY-MM-DD/<file_name>`
  allowing 100% offline testing with no cloud charges.

Why Date Partitioning in Bronze Matters:
Partitioning raw files by `ingestion_date=YYYY-MM-DD` guarantees auditability,
supports point-in-time reprocessing, and avoids accidental overwrites of previous ingestions.
"""

import hashlib
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from config.settings import AppConfig, get_config

logger = logging.getLogger(__name__)


def compute_sha256(file_path: Path, block_size: int = 65536) -> str:
    """
    Computes a cryptographic SHA-256 hash for a file.
    Used for verifying data integrity and tracking file versions in manifests.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            sha256.update(block)
    return sha256.hexdigest()


class BronzeIngestionService:
    """Service to handle landing raw data into the Bronze zone (S3 or Local)."""

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or get_config()
        self.storage_mode = self.config.storage.storage_mode.lower()
        self.s3_client = None

        if self.storage_mode == "s3":
            # Initialize Boto3 S3 Client using credentials or IAM instance profile
            session_kwargs = {"region_name": self.config.aws_region}
            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            self.s3_client = boto3.client("s3", **session_kwargs)
            logger.info(f"Initialized Boto3 S3 client for region: {self.config.aws_region}")
        else:
            logger.info("BronzeIngestionService running in LOCAL filesystem mode.")

    def ingest_file(
        self,
        source_file_path: Path,
        dataset_name: str,
        ingestion_date: Optional[str] = None
    ) -> Tuple[bool, str, Dict[str, any]]:
        """
        Ingests a single source file into the Bronze layer under:
        bronze/<dataset_name>/ingestion_date=YYYY-MM-DD/<source_file_name>

        Args:
            source_file_path: Local Path to validated raw CSV.
            dataset_name: Logical entity name (e.g., 'orders', 'order_items').
            ingestion_date: Date string YYYY-MM-DD (defaults to today UTC).

        Returns:
            Tuple of (success: bool, target_uri: str, file_details: dict)
        """
        if not ingestion_date:
            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        file_name = source_file_path.name
        checksum = compute_sha256(source_file_path)
        file_size_bytes = source_file_path.stat().st_size

        subpath = f"{dataset_name}/ingestion_date={ingestion_date}/{file_name}"
        details = {
            "source_file": str(source_file_path),
            "file_name": file_name,
            "dataset": dataset_name,
            "ingestion_date": ingestion_date,
            "sha256": checksum,
            "size_bytes": file_size_bytes,
            "storage_mode": self.storage_mode
        }

        if self.storage_mode == "s3":
            # [Step 1] Upload to S3
            bucket = self.config.storage.s3_bucket
            s3_key = f"{self.config.storage.zones.get('bronze', 'bronze')}/{subpath}"
            target_uri = f"s3://{bucket}/{s3_key}"

            try:
                logger.info(f"Uploading {source_file_path} to {target_uri}...")
                self.s3_client.upload_file(
                    Filename=str(source_file_path),
                    Bucket=bucket,
                    Key=s3_key,
                    ExtraArgs={"ServerSideEncryption": "AES256"}  # Enforce at-rest encryption
                )
                details["target_uri"] = target_uri
                logger.info(f"✓ S3 Upload successful: {target_uri}")
                return True, target_uri, details

            except ClientError as e:
                err_msg = f"S3 Upload failed for {source_file_path}: {e.response['Error']['Message']}"
                logger.error(err_msg)
                details["error"] = err_msg
                return False, "", details
        else:
            # [Step 2] Copy to Local Bronze Directory
            target_file_path = Path(self.config.get_zone_path("bronze", subpath))
            target_file_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                shutil.copy2(source_file_path, target_file_path)
                target_uri = str(target_file_path)
                details["target_uri"] = target_uri
                logger.info(f"✓ Local Bronze copy complete: {target_uri}")
                return True, target_uri, details

            except Exception as e:
                err_msg = f"Local copy failed: {str(e)}"
                logger.error(err_msg)
                details["error"] = err_msg
                return False, "", details

