"""
Source File Validation Module
=============================
This module verifies raw incoming CSV files before they are ingested into the
Bronze data lake layer.

Validation Rules Enforced:
1. File Existence: Ensure the source file exists at the expected path.
2. File Non-Empty: Verify file size is greater than 0 bytes.
3. Header Integrity: Validate that all declared mandatory columns are present in the header.
4. Non-Empty Rows: Ensure the file contains at least one data record beyond the header row.

Why This Matters in Production:
Preventing malformed or zero-byte files from reaching Bronze protects downstream
transformations from unexpected failures, preserving compute budget in Glue/EMR.
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def validate_file_structure(file_path: Path, mandatory_columns: List[str]) -> Tuple[bool, str, Dict[str, any]]:
    """
    Validates that a CSV file exists, is non-empty, and contains all required headers.

    Args:
        file_path: Path to the CSV file on disk.
        mandatory_columns: List of column names that must exist in the header.

    Returns:
        Tuple of (is_valid: bool, error_message: str, metadata: dict)
    """
    metadata = {
        "file_name": file_path.name,
        "exists": False,
        "byte_size": 0,
        "row_count": 0,
        "columns_found": [],
        "missing_columns": []
    }

    # [Check 1] Check file existence
    if not file_path.exists():
        msg = f"Validation Failed: File does not exist at {file_path}"
        logger.error(msg)
        return False, msg, metadata

    metadata["exists"] = True
    metadata["byte_size"] = file_path.stat().st_size

    # [Check 2] Check for zero-byte files
    if metadata["byte_size"] == 0:
        msg = f"Validation Failed: File {file_path.name} is empty (0 bytes)."
        logger.error(msg)
        return False, msg, metadata

    # [Check 3] Read header and count rows
    try:
        with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)

            if not header:
                msg = f"Validation Failed: File {file_path.name} has no header line."
                logger.error(msg)
                return False, msg, metadata

            # Strip whitespace and normalize header names
            header = [col.strip() for col in header]
            metadata["columns_found"] = header

            # Validate mandatory columns
            missing = [col for col in mandatory_columns if col not in header]
            if missing:
                metadata["missing_columns"] = missing
                msg = f"Validation Failed: {file_path.name} is missing mandatory columns: {missing}"
                logger.error(msg)
                return False, msg, metadata

            # Count rows
            row_count = sum(1 for _ in reader)
            metadata["row_count"] = row_count

            if row_count == 0:
                msg = f"Validation Failed: File {file_path.name} contains headers but 0 data rows."
                logger.warning(msg)
                return False, msg, metadata

    except Exception as exc:
        msg = f"Validation Failed: Error reading {file_path.name}: {str(exc)}"
        logger.exception(msg)
        return False, msg, metadata

    logger.info(f"✓ {file_path.name} passed validation: {metadata['row_count']} rows, {metadata['byte_size']} bytes.")
    return True, "Success", metadata

