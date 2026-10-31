# System Architecture & Technical Deep-Dive

## 1. End-to-End Pipeline Architecture

```text
                  SOURCE LAYER: OLIST BRAZILIAN E-COMMERCE
       8 Relational CSV Files: orders, items, payments, customers,
       products, sellers, reviews, category_translation
                                  │
                                  ▼
                     [1] INGESTION & AUDIT ENGINE
       - Mandatory header validation & non-empty byte checks
       - SHA-256 cryptographic checksum calculation
       - Immutable JSON Ingestion Manifest generation
                                  │
                                  ▼
                       [2] BRONZE LAYER (RAW)
       - s3://<bucket>/bronze/<dataset>/ingestion_date=YYYY-MM-DD/
       - Preserves source data verbatim with zero destructive edits
                                  │
                                  ▼
               [3] SILVER TRANSFORMATION & DATA QUALITY
       - Timestamp parsing to ISO-8601 UTC
       - Snake_case header normalization & type casting
       - Primary key deduplication
       - Domain & range checks (price >= 0, review score 1-5)
       - Chronological ordering verification (delivered >= purchase)
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
                 ▼                                 ▼
      [4] SILVER ZONE (PARQUET)           QUARANTINE ZONE
      - Columnar Snappy Parquet          - Rejection metadata & error codes
      - orders partitioned by:           - s3://<bucket>/quarantine/<table_name>/
        year=YYYY/month=MM/
                 │
                 ▼
          [5] GOLD ZONE: KIMBALL STAR SCHEMA & MARTS
      - Conformed Dimensions: dim_customers, dim_products, dim_sellers, dim_date
      - Fact Tables: fact_order_items, fact_payments
      - Pre-Aggregated Marts: gold_daily_sales, gold_product_performance, gold_delivery_kpis
                 │
                 ▼
         [6] METASTORE: AWS GLUE DATA CATALOG
      - Database: ecommerce_lakehouse_db
      - Serverless schema discovery & partition indexing
                 │
                 ▼
          [7] SERVING LAYER: AMAZON ATHENA & BI
      - Serverless distributed Presto SQL queries
      - Integrated with Amazon QuickSight / Tableau / PowerBI
```

---

## 2. Trace of a Record's Journey

Let's follow order record `e481f51cbdc54678b7cc49136f2d6af7` through the entire platform:

1. **Source Landing:** Arrives as raw text inside `olist_orders_dataset.csv`.
2. **Ingestion & Bronze:** `ingestion.validate_source` checks that `order_id`, `customer_id`, and `order_purchase_timestamp` are non-null. `upload_to_s3` calculates SHA-256 hash `d41d8cd9...` and lands the file at `s3://<bucket>/bronze/orders/ingestion_date=2026-10-31/olist_orders_dataset.csv`. The run manifest logs the ingestion.
3. **Silver Transformation:** `transformations.clean_orders` parses `2017-10-02 10:56:33` into UTC timestamp, verifies delivery timestamp `2017-10-10` is after purchase, deduplicates on `order_id`, and writes Snappy Parquet to `silver/orders/year=2017/month=10/part-*.parquet`.
4. **Gold Dimensional Modeling:** In `modeling.fact_tables`, the order's items and payments are merged into `fact_order_items` and `fact_payments`, tagged with foreign key `order_date_key = 20171002`. In `modeling.build_gold_marts`, the order's total items and revenues are safely aggregated without fan-out into `gold_daily_sales`.
5. **Athena Querying:** When an analyst executes:
   ```sql
   SELECT total_orders, gross_revenue FROM gold_daily_sales WHERE order_date = DATE '2017-10-02';
   ```
   Athena uses partition pruning and columnar projection, scanning only a few kilobytes from S3 to return the answer in sub-seconds.

---

## 3. Data Lake vs. Data Warehouse Trade-Offs

| Architectural Factor | Amazon S3 Data Lake (Our Design) | Traditional Data Warehouse (e.g. Redshift / Snowflake) |
| :--- | :--- | :--- |
| **Storage Cost** | \$0.023 / GB / month (Standard S3) | Compute-bound node hours (\$0.25 - \$4.00+ / hr) |
| **Storage & Compute Decoupling** | Complete decoupling. Scale storage independently of compute. | Coupled or semi-coupled compute clusters. |
| **Supported Data Types** | Unstructured, semi-structured (JSON), and structured (Parquet/CSV). | Structured tabular relational schemas. |
| **Query Engine** | Serverless SQL (Athena Presto, EMR Spark, Glue). Pay only for data scanned. | Dedicated cluster compute instances. |

---

## 4. Cost Optimization Architecture

1. **Parquet Columnar Format:** Parquet groups data by column instead of row, allowing Athena to only read columns requested in the query.
2. **Snappy Compression:** Shrinks file sizes by 70-80% compared to raw CSV, directly lowering Athena query scan costs (\$5.00 / TB scanned).
3. **Partition Pruning:** Hive-style date partitioning (`year=YYYY/month=MM/`) ensures queries filtering by date skip 90%+ of S3 bucket prefixes entirely.
