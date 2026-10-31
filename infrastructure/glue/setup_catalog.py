"""
AWS Glue Data Catalog Setup Script
==================================
Automates the provisioning of:
1. Glue Data Catalog Database: `ecommerce_lakehouse_db`
2. Glue Crawlers:
   - `silver_data_crawler`: Discovers Silver Parquet tables & year/month partitions.
   - `gold_data_crawler`: Discovers Gold facts, dimensions, and aggregated marts.

Why Glue Data Catalog Matters:
The Glue Data Catalog serves as the central Apache Hive-compatible metastore in AWS.
Amazon Athena queries use this metastore to resolve table locations on S3, column schemas,
and partition keys without needing persistent database servers.

Usage:
    python -m infrastructure.glue.setup_catalog --bucket your-ecommerce-bucket --role-arn arn:aws:iam::123456789012:role/AWSGlueServiceRole
"""

import argparse
import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("infrastructure.glue")

DATABASE_NAME = "ecommerce_lakehouse_db"


def create_glue_database(glue_client, database_name: str = DATABASE_NAME) -> bool:
    """Creates the Glue database if it doesn't already exist."""
    try:
        logger.info(f"Checking for Glue database: {database_name}...")
        glue_client.get_database(Name=database_name)
        logger.info(f"✓ Database '{database_name}' already exists.")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info(f"Creating Glue database: '{database_name}'...")
            glue_client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": "Central metadata catalog for E-Commerce Data Lakehouse"
                }
            )
            logger.info(f"✓ Successfully created database '{database_name}'.")
            return True
        else:
            logger.error(f"Error checking database: {e}")
            return False


def create_glue_crawler(
    glue_client,
    crawler_name: str,
    s3_path: str,
    iam_role_arn: str,
    database_name: str = DATABASE_NAME
) -> bool:
    """Creates or updates a Glue Crawler pointing to an S3 zone path."""
    try:
        logger.info(f"Configuring Glue Crawler: {crawler_name} for {s3_path}...")
        crawler_config = {
            "Name": crawler_name,
            "Role": iam_role_arn,
            "DatabaseName": database_name,
            "Targets": {"S3Targets": [{"Path": s3_path}]},
            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            }
        }

        try:
            glue_client.get_crawler(Name=crawler_name)
            logger.info(f"Updating existing crawler: {crawler_name}")
            glue_client.update_crawler(**crawler_config)
        except ClientError as ce:
            if ce.response["Error"]["Code"] == "EntityNotFoundException":
                logger.info(f"Creating new crawler: {crawler_name}")
                glue_client.create_crawler(**crawler_config)
            else:
                raise

        logger.info(f"✓ Crawler '{crawler_name}' configured successfully.")
        return True

    except Exception as exc:
        logger.error(f"Failed to configure crawler {crawler_name}: {exc}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Set up AWS Glue Data Catalog.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--role-arn", required=True, help="IAM Role ARN for AWS Glue")
    parser.add_argument("--region", default="us-east-1", help="AWS Region (default: us-east-1)")
    args = parser.parse_args()

    client = boto3.client("glue", region_name=args.region)

    # [1] Create Database
    if not create_glue_database(client):
        return

    # [2] Create Crawlers for Silver & Gold
    create_glue_crawler(
        glue_client=client,
        crawler_name="ecommerce_silver_crawler",
        s3_path=f"s3://{args.bucket}/silver/",
        iam_role_arn=args.role_arn
    )

    create_glue_crawler(
        glue_client=client,
        crawler_name="ecommerce_gold_crawler",
        s3_path=f"s3://{args.bucket}/gold/",
        iam_role_arn=args.role_arn
    )

    logger.info("Glue Data Catalog infrastructure provisioning completed.")


if __name__ == "__main__":
    main()
