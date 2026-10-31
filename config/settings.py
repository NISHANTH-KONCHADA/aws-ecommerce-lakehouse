"""
Configuration Settings Loader
=============================
This module is responsible for loading environment variables and YAML configurations
for the AWS E-Commerce Data Lake platform.

It supports dual execution modes:
1. `STORAGE_MODE=local`: Files are read and written to local filesystem directories
   (e.g., ./data/bronze, ./data/silver, etc.) for local testing with zero AWS costs.
2. `STORAGE_MODE=s3`: Files are read and written directly to AWS S3 using boto3 / PySpark.

Usage:
    from config.settings import AppConfig, get_config

    config = get_config()
    bronze_path = config.get_zone_path("bronze", "orders")
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from dotenv import load_dotenv

# Automatically load variables from .env file if present
load_dotenv()


@dataclass
class StorageConfig:
    """Represents storage configuration and directory routing."""
    storage_mode: str  # 'local' or 's3'
    s3_bucket: str
    local_data_dir: Path
    zones: Dict[str, str] = field(default_factory=dict)

    def get_path(self, zone: str, subpath: str = "") -> str:
        """
        Resolves the appropriate URI for a specific zone and subpath.

        Args:
            zone: One of 'bronze', 'silver', 'gold', 'quarantine', 'manifests'.
            subpath: Optional folder or file subpath (e.g., 'orders/year=2018').

        Returns:
            A string URI (e.g., 's3://my-bucket/silver/orders' or './data/silver/orders').
        """
        prefix = self.zones.get(zone, zone)
        clean_subpath = subpath.strip("/\\")

        if self.storage_mode.lower() == "s3":
            # Construct standard S3 URI
            if clean_subpath:
                return f"s3://{self.s3_bucket}/{prefix}/{clean_subpath}"
            return f"s3://{self.s3_bucket}/{prefix}"
        else:
            # Construct standard local filesystem path
            target_dir = self.local_data_dir / prefix
            if clean_subpath:
                return str(target_dir / clean_subpath)
            return str(target_dir)


@dataclass
class DatasetConfig:
    """Represents configuration metadata for a single dataset entity."""
    name: str
    source_file: str
    silver_table: str
    primary_key: List[str]
    mandatory_columns: List[str]
    timestamp_columns: List[str] = field(default_factory=list)
    numeric_columns: Dict[str, str] = field(default_factory=dict)
    partition_by: List[str] = field(default_factory=list)


@dataclass
class AppConfig:
    """Master application configuration container."""
    aws_region: str
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    environment: str
    log_level: str
    storage: StorageConfig
    datasets: Dict[str, DatasetConfig]
    data_quality: Dict[str, Any]

    def get_zone_path(self, zone: str, subpath: str = "") -> str:
        """Convenience shortcut to resolve zone URI."""
        return self.storage.get_path(zone, subpath)


def load_yaml_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Reads and parses the YAML configuration file."""
    if config_path is None:
        # Default to config/config.yaml relative to project root
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_config(config_path: Optional[Path] = None) -> AppConfig:
    """
    Factory function to build and return an AppConfig instance
    populated from environment variables and config.yaml.
    """
    raw_yaml = load_yaml_config(config_path)
    base_dir = Path(__file__).resolve().parent.parent

    # Read environment variables with fallback to config.yaml defaults
    storage_mode = os.getenv("STORAGE_MODE", "local").lower()
    s3_bucket = os.getenv("S3_BUCKET_NAME", raw_yaml.get("storage", {}).get("s3_bucket", "ecommerce-lakehouse"))
    local_data_dir = Path(os.getenv("LOCAL_DATA_DIR", str(base_dir / "data")))

    zones = raw_yaml.get("storage", {}).get("zones", {
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold",
        "quarantine": "quarantine",
        "manifests": "manifests",
        "athena_results": "athena-results"
    })

    storage = StorageConfig(
        storage_mode=storage_mode,
        s3_bucket=s3_bucket,
        local_data_dir=local_data_dir,
        zones=zones
    )

    # Parse dataset definitions
    datasets = {}
    for ds_name, ds_info in raw_yaml.get("datasets", {}).items():
        datasets[ds_name] = DatasetConfig(
            name=ds_name,
            source_file=ds_info.get("source_file", f"{ds_name}.csv"),
            silver_table=ds_info.get("silver_table", ds_name),
            primary_key=ds_info.get("primary_key", []),
            mandatory_columns=ds_info.get("mandatory_columns", []),
            timestamp_columns=ds_info.get("timestamp_columns", []),
            numeric_columns=ds_info.get("numeric_columns", {}),
            partition_by=ds_info.get("partition_by", [])
        )

    return AppConfig(
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        environment=os.getenv("ENVIRONMENT", "dev"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        storage=storage,
        datasets=datasets,
        data_quality=raw_yaml.get("data_quality", {})
    )

