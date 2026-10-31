# AWS E-Commerce Data Lake & Analytics Platform

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.14-blue)](https://www.python.org/)
[![Cloud](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-orange)](https://aws.amazon.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-green)](#architecture-overview)
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
| **Bronze (Raw)** | Source CSV | Exact copy of incoming source files organized by `ingestion_date=YYYY-MM-DD`. Ingestion manifest tracks file sizes, row counts, and SHA-256 checksums. |
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
│   ├── architecture.md      # Record journey, S3 layout, cost optimizations
│   ├── data_dictionary.md   # Schemas, primary/foreign keys, and declared grains
│   └── decisions.md         # Architecture Decision Records (ADRs)
├── ingestion/               # Bronze ingestion, manifest generator, S3 uploader
├── transformations/         # Silver PySpark data cleaning & standardization
├── modeling/                # Gold dimensional modeling (facts, dims, marts)
├── data_quality/            # Reusable validation rules & quarantine handler
├── glue_jobs/               # Unified AWS Glue PySpark job scripts
├── infrastructure/          # IAM policies and Glue Catalog setup scripts
├── sql/                     # Athena SQL analytical queries for business KPIs
├── tests/                   # Pytest test suite for transformations & quality rules
├── requirements.txt         # Pinned Python package dependencies
├── .env.example             # Configuration environment variable template
└── .gitignore               # Strict gitignore for credentials and raw data
```

---

## Quickstart & Local Setup

### 1. Set Up Environment
```bash
# Clone the repository
git clone https://github.com/NISHANTH-KONCHADA/aws-ecommerce-lakehouse.git
cd aws-ecommerce-lakehouse

# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1   # Windows PowerShell
# source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Configure settings
cp .env.example .env
```

### 2. Run the Full Local Pipeline (Zero AWS Charges)
```bash
# [Step 1] Profile source datasets
python -m ingestion.profile_source --data-dir ./data/sample

# [Step 2] Ingest raw files to Bronze layer (with SHA-256 checksums & manifests)
python -m ingestion.extract --source-dir ./data/sample

# [Step 3] Transform Bronze CSVs to Silver Parquet (with Data Quality & Quarantine)
python -m transformations.runner

# [Step 4] Build Gold Kimball Star Schema & Aggregated Analytics Marts
python -m modeling.build_gold_marts

# [Step 5] Run Automated Test Suite
pytest -v tests/
```

### 3. Deploy to AWS (Cloud Mode)
1. Set `STORAGE_MODE=s3` and fill in `S3_BUCKET_NAME` in `.env`.
2. Provision Glue Catalog database and crawlers:
   ```bash
   python -m infrastructure.glue.setup_catalog --bucket your-bucket-name --role-arn arn:aws:iam::123456789012:role/AWSGlueServiceRole
   ```
3. Deploy `glue_jobs/ecommerce_etl.py` to AWS Glue and schedule via AWS Step Functions or EventBridge.
4. Execute analytical queries in `sql/` directly via Amazon Athena.

---

## Core Business Questions Answered

| Business Question | Athena SQL Query | Key Finding / KPI |
| :--- | :--- | :--- |
| **Revenue Trend & MoM Growth** | `sql/01_monthly_orders_and_revenue.sql` | Monthly order volume, item revenue, freight, gross revenue, and AOV. |
| **Top Categories & Products** | `sql/02_top_products_and_categories.sql` | Revenue shares, volume ranking via dense ranking. |
| **Geographic Distribution** | `sql/03_orders_by_customer_state.sql` | Order concentration by Brazilian states (SP, RJ, MG). |
| **Payment Method Preference** | `sql/04_payment_methods_distribution.sql` | Credit card vs. boleto volume, installment dynamics. |
| **Delivery SLAs & Late Rates** | `sql/05_delivery_performance_and_late_rates.sql` | Late delivery rate percentage and average transit latency. |
| **Customer Retention** | `sql/06_repeat_customers_analysis.sql` | Repeat purchase cohorts via `customer_unique_id`. |

---

## Interview Questions & Architectural Mastery

<details>
<summary><b>1. Why choose Amazon S3 as the data lake instead of a Data Warehouse?</b></summary>
Amazon S3 decouples compute and storage completely. Storage costs are extremely low (\$0.023/GB/month) with 99.999999999% (11 9s) durability. S3 allows ingesting raw, semi-structured, and columnar formats in parallel, allowing compute engines (PySpark, Glue, Athena, Presto) to spin up only on demand.
</details>

<details>
<summary><b>2. Why use Apache Parquet instead of CSV in Silver and Gold?</b></summary>
Parquet is a columnar storage format with built-in Snappy compression and dictionary encoding. Amazon Athena charges \$5.00 per TB of data scanned. By converting CSV to Parquet:
- File sizes shrink by 75-80%.
- Athena only scans columns referenced in the query (column pruning), reducing query runtimes and costs by up to 90%.
</details>

<details>
<summary><b>3. How do you avoid many-to-many fan-out between order items and payments?</b></summary>
An order can contain $N$ line items and $M$ payment installments. A naive SQL join on `order_id` causes a Cartesian product of $N \times M$ rows, artificially inflating sums of price and payment_value. We maintain separate fact tables at declared grains (`fact_order_items` at grain `order_id + order_item_id` and `fact_payments` at grain `order_id + payment_sequential`), and aggregate each table to `order_id` level before joining.
</details>

<details>
<summary><b>4. How is pipeline idempotency guaranteed?</b></summary>
Every batch ingestion writes to partitioned paths with deterministic partition keys (`ingestion_date=YYYY-MM-DD` in Bronze, `year=YYYY/month=MM/` in Silver). Transformations use dynamic partition overwrite mode (`spark.sql.sources.partitionOverwriteMode=dynamic`), guaranteeing that re-running a batch overwrites only that specific partition without generating duplicate records.
</details>

---

## License
MIT License. Developed for educational and production demonstration of AWS Batch Data Engineering.
