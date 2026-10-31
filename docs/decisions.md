# Architecture Decision Records (ADR)

## ADR 001: Medallion Architecture (Bronze, Silver, Gold) on Amazon S3

- **Status:** Accepted
- **Context:**
  The platform processes batch e-commerce data from multiple disparate sources (orders, order items, payments, customers, products, reviews). Business users need fast, aggregated analytical queries without compromising raw auditability.
- **Decision:**
  Implement a three-tier Medallion architecture on Amazon S3:
  1. **Bronze (Raw Zone):** Store incoming CSV files verbatim with partition path `ingestion_date=YYYY-MM-DD`. Generate a JSON manifest tracking SHA256 hashes, byte sizes, and row counts to ensure raw data integrity.
  2. **Silver (Cleaned Zone):** Parse timestamps into ISO-8601 UTC, normalize column headers to snake_case, enforce data types, deduplicate records based on declared entity keys, and store in columnar Snappy-compressed Parquet.
  3. **Gold (Curated Zone):** Construct dimensional star-schema tables (`dim_*`, `fact_*`) and pre-aggregated KPI tables (`daily_sales`, `delivery_kpis`) optimized for Athena SQL queries.
  4. **Quarantine Zone:** Divert non-conforming or corrupted records (negative payment amounts, corrupted foreign keys) into a separate path with detailed error codes.
- **Consequences:**
  - Decouples raw data landing from analytical serving.
  - Allows full reprocessing from Bronze if business rules evolve.
  - Significantly reduces query execution costs in Athena via Parquet columnar projection and partition pruning.

---

## ADR 002: Snappy-Compressed Parquet Format for Silver & Gold

- **Status:** Accepted
- **Context:**
  Athena charges \$5.00 per TB of data scanned. CSV files require scanning all bytes and columns for every query.
- **Decision:**
  Store Silver and Gold data in Apache Parquet format with Snappy compression.
- **Consequences:**
  - Column pruning: Athena scans only columns referenced in `SELECT` or `WHERE`.
  - Compression reduces S3 storage costs by up to 75-80%.
  - Built-in schema metadata and statistics (min/max per column block) enable faster predicate pushdown.

---

## ADR 003: Star Schema vs. Wide Flat Table for Orders & Payments

- **Status:** Accepted
- **Context:**
  An order can contain multiple items and multiple payment installments. Joining `olist_order_items` and `olist_order_payments` naively at order level results in a Cartesian fan-out ($N \times M$), artificially multiplying revenue and payment totals.
- **Decision:**
  Maintain separate fact tables (`fact_order_items` at grain `order_id + order_item_id` and `fact_payments` at grain `order_id + payment_sequential`). Aggregate at the required grain before joining, and build dedicated pre-aggregated Gold marts for joined reporting.
- **Consequences:**
  - Prevents revenue calculation errors.
  - Maintains strict mathematical accuracy across financial reporting.
