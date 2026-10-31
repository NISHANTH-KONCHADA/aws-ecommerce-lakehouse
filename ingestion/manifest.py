"""
Ingestion Manifest Generator
============================
Creates structured, immutable audit manifests for every ingestion batch.

A manifest records:
- Pipeline run ID (UUID)
- Execution start & completion timestamps (ISO-8601 UTC)
- Environment and storage mode (local vs. S3)
- Granular file inventory:
  - Source path, destination URI, dataset name
  - SHA-256 cryptographic checksum
  - File size in bytes and data row count
  - Validation & ingestion status

Why Ingestion Manifests Matter in Data Engineering:
1. Auditability & Compliance: Proof of exactly what data entered the lake and when.
2. Idempotency & De-duplication: Enables pipeline jobs to skip reprocessing identical files.
3. Troubleshooting: Immediate reference for reconciling missing or mutated rows.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from config.settings import AppConfig, get_config

logger = logging.getLogger(__name__)


class IngestionManifest:
    """Manages creation and persisting of ingestion run manifests."""

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or get_config()
        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.records: List[Dict[str, any]] = []

    def record_file(
        self,
        dataset_name: str,
        source_path: str,
        target_uri: str,
        sha256: str,
        size_bytes: int,
        row_count: int,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Appends metadata for an individual ingested file."""
        self.records.append({
            "dataset": dataset_name,
            "source_path": source_path,
            "target_uri": target_uri,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "row_count": row_count,
            "status": status,
            "error_message": error_message
        })

    def save_manifest(self) -> str:
        """
        Finalizes manifest and writes it to S3 or local manifests folder.

        Returns:
            The URI/path where the manifest was persisted.
        """
        completed_at = datetime.now(timezone.utc).isoformat()
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        manifest_filename = f"ingestion_manifest_{timestamp_str}_{self.run_id[:8]}.json"

        manifest_payload = {
            "pipeline_name": "aws-ecommerce-lakehouse",
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": completed_at,
            "storage_mode": self.config.storage.storage_mode,
            "total_files_processed": len(self.records),
            "total_successful": sum(1 for r in self.records if r["status"] == "SUCCESS"),
            "total_failed": sum(1 for r in self.records if r["status"] != "SUCCESS"),
            "files": self.records
        }

        json_data = json.dumps(manifest_payload, indent=2)

        if self.config.storage.storage_mode.lower() == "s3":
            # Persist manifest to S3
            bucket = self.config.storage.s3_bucket
            s3_key = f"{self.config.storage.zones.get('manifests', 'manifests')}/{manifest_filename}"
            manifest_uri = f"s3://{bucket}/{s3_key}"

            session_kwargs = {"region_name": self.config.aws_region}
            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

            s3_client = boto3.client("s3", **session_kwargs)
            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=json_data,
                    ContentType="application/json",
                    ServerSideEncryption="AES256"
                )
                logger.info(f"✓ Manifest saved to S3: {manifest_uri}")
                return manifest_uri
            except ClientError as e:
                logger.error(f"Failed to save manifest to S3: {e}")
                raise
        else:
            # Persist manifest to Local Directory
            manifest_dir = Path(self.config.get_zone_path("manifests"))
            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / manifest_filename

            with open(manifest_path, "w", encoding="utf-8") as f:
                f.write(json_data)

            logger.info(f"✓ Manifest saved to local disk: {manifest_path}")
            return str(manifest_path)

