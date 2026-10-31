# AWS E-Commerce Data Lake & Analytics Platform

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![Cloud](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-orange)](https://aws.amazon.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-green)](#architecture)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end, production-grade batch data engineering platform built on the **Brazilian E-Commerce Public Dataset by Olist**. The platform ingests multi-source e-commerce records, enforces data quality checks, quarantines anomalous data, transforms datasets through Medallion architecture layers using PySpark, catalogs them with the AWS Glue Data Catalog, and enables analytical queries using Amazon Athena.

---

## Architecture Overview

```text
               OLIST E-COMMERCE SOURCE DATA
          (Orders, Items, Payments, Products, etc.)
                            │
                            ▼
               1. INGESTION ENGINE (Python)
         Checksum verification, file validation, manifests
                            │
                            ▼
               2. BRONZE LAYER (Amazon S3 / Local)
          Immutable raw storage, date-partitioned landing
                            │
                            ▼
           3. DATA QUALITY & CLEANING (PySpark)
       Normalization, timestamp parsing, schema enforcement
                            │
               ┌────────────┴────────────┐
               │                         │
               ▼                         ▼
      4. SILVER LAYER (Parquet)    QUARANTINE ZONE
      Cleaned, typed, deduplicated  Invalid records with
                                    error reason codes
               │
               ▼
      5. GOLD LAYER (Star Schema & Data Marts)
      Fact & Dimension tables (fact_order_items,
      dim_customers, daily_sales, delivery_kpis)
               │
               ▼
      6. AWS GLUE DATA CATALOG
      Metadata databases & schema crawling
               │
               ▼
      7. AMAZON ATHENA / SQL ANALYTICS
      Interactive serverless SQL analytics & KPI reports
```

---

## Medallion Architecture Layers

| Layer | Storage Format | Description |
| :--- | :--- | :--- |
| **Bronze (Raw)** | Source CSV | Exact copy of incoming source files organized by `ingestion_date=YYYY-MM-DD`. Ingestion manifest tracks file sizes, row counts, and SHA256 checksums. |
| **Silver (Cleaned)** | Snappy Parquet | Columnar format, standardized snake_case column names, parsed ISO UTC timestamps, deduplicated keys, and filtered invalid records. |
| **Gold (Curated)** | Snappy Parquet | Star schema dimensional tables (`dim_customers`, `dim_products`, `dim_sellers`, `dim_date`, `fact_order_items`, `fact_payments`) and aggregated business marts (`daily_sales`, `delivery_kpis`). |
| **Quarantine** | JSON / Parquet | Records failing data validation checks (e.g. invalid negative payments, orphaned foreign keys, review score outliers) with diagnostic metadata. |

---

## Repository Structure

```text
├── config/                  # Configuration files (YAML, settings loader)
├── data/                    # Sample synthetic data and download instructions
│   └── sample/              # Micro-sample CSVs for instant local testing
├── docs/                    # Architecture deep-dive, data dictionary, decision logs
├── ingestion/               # Bronze ingestion, manifest generator, S3 uploader
├── transformations/         # Silver PySpark data cleaning & standardization
├── modeling/                # Gold dimensional modeling (facts, dims, marts)
├── data_quality/            # Reusable validation rules & quarantine handler
├── glue_jobs/               # Unified AWS Glue PySpark job scripts
├── sql/                     # Athena SQL analytical queries for business KPIs
├── tests/                   # Pytest test suite for transformations & quality rules
├── requirements.txt         # Pinned Python package dependencies
├── .env.example             # Configuration environment variable template
└── .gitignore               # Strict gitignore for credentials and raw data
```

---

## Quickstart & Local Setup

### 1. Clone & Set Up Python Environment
```bash
git clone https://github.com/NISHANTH-KONCHADA/aws-ecommerce-lakehouse.git
cd aws-ecommerce-lakehouse

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables
```bash
cp .env.example .env
```
By default, `STORAGE_MODE=local` is set, allowing you to run and verify the entire pipeline on your local machine using sample datasets with zero AWS charges!

---

## Key Business Insights Enabled by Platform
- **Revenue & Orders:** Monthly sales velocity, average order value (AOV), and repeat customer rates.
- **Logistics & Delivery:** Late delivery rates, delivery duration by customer state.
- **Product & Category Performance:** Top revenue-driving categories and sellers.
- **Payment Behavior:** Installment trends and preferred payment methods.
