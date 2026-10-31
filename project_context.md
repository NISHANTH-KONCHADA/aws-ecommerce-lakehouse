# Project 1: AWS E-Commerce Data Lake & ETL Analytics Platform

> **Project type:** Batch data engineering\
> **Level:** Intermediate to advanced\
> **Primary stack:** Python, SQL, Amazon S3, AWS Glue, PySpark, Glue
> Data Catalog, Amazon Athena\
> **Optional extension:** Amazon Redshift, Step Functions, CloudWatch,
> Terraform\
> **Suggested duration:** 2--3 weeks for a solid implementation\
> **Dataset:** Brazilian E-Commerce Public Dataset by Olist

------------------------------------------------------------------------

## 1. Project objective

Build an end-to-end batch data engineering platform for an e-commerce
business. The platform should ingest data from multiple source files,
preserve the original data, clean and validate it, transform it into
analytics-ready datasets, and make those datasets queryable using SQL.

The goal is not simply to upload CSV files to S3. The goal is to
demonstrate the complete data engineering lifecycle:

1.  Understand source data and business requirements.
2.  Ingest data reliably.
3.  Store raw data without changing its meaning.
4.  clean, standardize and validate records.
5.  Model data for analytical queries.
6.  Transform data using PySpark and AWS Glue.
7.  Catalog datasets and query them using Athena.
8.  Monitor failures, measure data quality and document the pipeline.

### Business scenario

An online retailer receives data about customers, orders, products,
sellers, payments, reviews and deliveries. These datasets are maintained
separately. Business teams need reliable answers to questions such as:

-   What is the monthly order and revenue trend?
-   Which products and categories generate the most revenue?
-   Which states and cities contribute the most orders?
-   What is the average order value?
-   How frequently are orders delivered late?
-   Which payment methods are used most often?
-   How many customers make repeat purchases?

The proposed platform consolidates these datasets into a governed data
lake and produces trusted analytical tables.

------------------------------------------------------------------------

## 2. Scope and expected outcome

### Minimum viable version

The first working version must:

-   Read the Olist CSV files.
-   Inspect schemas, nulls, duplicates and key relationships.
-   Upload original files to an S3 `bronze` zone.
-   Transform selected datasets with Python/PySpark.
-   Write cleaned data in Parquet format to a `silver` zone.
-   Produce at least three analytical datasets in a `gold` zone.
-   Register tables in AWS Glue Data Catalog.
-   Query the tables using Athena.
-   Include validation checks, logs, tests and a clear README.

### Advanced version

After the minimum viable version works, add:

-   Incremental ingestion and processing.
-   Date-based partitioning.
-   Data-quality quarantine for invalid records.
-   Schema-change handling.
-   Orchestration with Glue Workflows or Step Functions.
-   CloudWatch monitoring and alarms.
-   Redshift as an additional warehouse destination.
-   Infrastructure as code using Terraform or AWS SAM.

Do not add every advanced service before the core pipeline works.

------------------------------------------------------------------------

## 3. Dataset

Use the **Brazilian E-Commerce Public Dataset by Olist**.

Dataset page:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The dataset contains multiple related CSV files, including:

  -----------------------------------------------------------------------------
  Source file                               Purpose
  ----------------------------------------- -----------------------------------
  `olist_orders_dataset.csv`                Order status and lifecycle
                                            timestamps

  `olist_order_items_dataset.csv`           Products, sellers, price and
                                            freight per order

  `olist_order_payments_dataset.csv`        Payment type, installments and
                                            payment value

  `olist_customers_dataset.csv`             Customer identifiers and location

  `olist_products_dataset.csv`              Product attributes and category

  `olist_sellers_dataset.csv`               Seller identifiers and location

  `olist_order_reviews_dataset.csv`         Review score and review details

  `product_category_name_translation.csv`   Portuguese-to-English category
                                            names

  `olist_geolocation_dataset.csv`           Geographic coordinates and
                                            postal-code information
  -----------------------------------------------------------------------------

Read the dataset's license and terms before redistributing it. Do not
commit the full dataset to GitHub unless its license permits it. Include
a download guide and small synthetic sample files instead.

### Initial exploration

Before building the AWS pipeline:

1.  Count rows in each file.
2.  List columns and data types.
3.  Identify null values.
4.  Check duplicate primary keys.
5.  Check whether order IDs and customer IDs join as expected.
6.  Inspect date formats and invalid timestamps.
7.  Record the findings in `docs/data_dictionary.md`.

Do not assume that every table has one row per order. For example, an
order can contain multiple order items and multiple payment records.

------------------------------------------------------------------------

## 4. Architecture

``` text
                 OLIST SOURCE DATA
          CSV files / optional source API
                       |
                       v
              PYTHON INGESTION
       schema checks, file checks, manifest
                       |
                       v
                AMAZON S3
              BRONZE / RAW
          Original source files preserved
                       |
                       v
             AWS GLUE / PYSPARK
       parsing, cleaning, validation, joins
                       |
              +--------+---------+
              |                  |
              v                  v
       SILVER / CLEAN       QUARANTINE
       standardized data    invalid records
              |
              v
       GOLD / CURATED TABLES
       daily sales, products,
       customers, delivery KPIs
              |
              v
       GLUE DATA CATALOG
       database and table metadata
              |
              v
          AMAZON ATHENA
        analytical SQL queries
              |
              v
       REPORTS / BUSINESS INSIGHTS
```

### Data zones

**Bronze -- raw**

-   Preserve source records as received.
-   Keep original file names or source identifiers.
-   Add ingestion metadata where practical.
-   Avoid destructive updates.
-   Organize files by source and ingestion date.

**Silver -- cleaned**

-   Standardize column names and types.
-   Parse dates and timestamps.
-   Handle invalid records.
-   Remove duplicates according to documented rules.
-   Store data in Parquet format.
-   Preserve traceability to the source.

**Gold -- analytics-ready**

-   Create business-level datasets.
-   Use consistent metric definitions.
-   Build aggregated tables for common questions.
-   Document each table's grain and business meaning.

Bronze, Silver and Gold are logical data zones, not AWS products.

------------------------------------------------------------------------

## 5. Suggested S3 layout

Use a bucket name that is globally unique. For example,
`nishanth-ecommerce-data-lake-<unique-suffix>` is only a naming pattern;
choose your own unique name.

``` text
s3://<bucket-name>/
|
|-- bronze/
|   |-- orders/ingestion_date=YYYY-MM-DD/
|   |-- order_items/ingestion_date=YYYY-MM-DD/
|   |-- payments/ingestion_date=YYYY-MM-DD/
|   |-- customers/ingestion_date=YYYY-MM-DD/
|   |-- products/ingestion_date=YYYY-MM-DD/
|   |-- sellers/ingestion_date=YYYY-MM-DD/
|   |-- reviews/ingestion_date=YYYY-MM-DD/
|   |-- category_translation/ingestion_date=YYYY-MM-DD/
|
|-- silver/
|   |-- orders/
|   |-- order_items/
|   |-- payments/
|   |-- customers/
|   |-- products/
|   |-- sellers/
|   |-- reviews/
|
|-- gold/
|   |-- daily_sales/
|   |-- product_performance/
|   |-- customer_analytics/
|   |-- delivery_performance/
|   |-- payment_analytics/
|
|-- quarantine/
|   |-- invalid_orders/
|   |-- invalid_payments/
|
|-- scripts/
|-- manifests/
|-- athena-results/
```

Use environment variables or a configuration file for bucket names and
paths. Do not hard-code credentials.

------------------------------------------------------------------------

## 6. Data model and table design

Before writing transformations, define the grain of each table. Grain
means what one row represents.

### Suggested fact and dimension tables

  -----------------------------------------------------------------------
  Table                   Grain                   Example key
  ----------------------- ----------------------- -----------------------
  `fact_order_items`      One row per order item  order_id +
                                                  order_item_id

  `fact_payments`         One row per payment     order_id +
                          record                  payment_sequential

  `dim_customers`         One row per customer    customer_id
                          record                  

  `dim_products`          One row per product     product_id

  `dim_sellers`           One row per seller      seller_id

  `dim_date`              One row per calendar    date_key
                          date                    
  -----------------------------------------------------------------------

A useful analytical model may contain `fact_order_items`,
`fact_payments`, `dim_customers`, `dim_products`, `dim_sellers` and
`dim_date`.

### Important modeling warning

Do not directly join order items and payments and then sum both `price`
and `payment_value` without understanding the relationship. An order can
have multiple items and multiple payment rows. A many-to-many expansion
can inflate totals.

Aggregate each fact table at its correct grain before joining, or keep
separate fact tables and join through dimensions.

### Example business definitions

Document the exact definitions used:

-   **Order count:** count of distinct eligible order IDs.
-   **Item revenue:** sum of item prices. State clearly whether freight
    is excluded.
-   **Freight revenue:** sum of freight values.
-   **Payment value:** sum of payment records, not automatically
    interchangeable with item revenue.
-   **Average order value:** defined revenue measure divided by distinct
    eligible orders.
-   **Late delivery:** delivered timestamp is later than estimated
    delivery timestamp.
-   **Delivery duration:** delivered timestamp minus purchase timestamp.

Choose which order statuses are included in each metric and explain the
choice.

------------------------------------------------------------------------

## 7. Implementation phases

### Phase 1 -- Local development and source profiling

**Tasks**

1.  Create a GitHub repository.
2.  Create a Python virtual environment.
3.  Install dependencies.
4.  Download and extract the dataset.
5.  Inspect CSV schemas using Python or Pandas.
6.  Create a data dictionary.
7.  Write basic validation scripts.

**Suggested dependencies**

``` text
pandas
pyarrow
boto3
pyspark
pytest
python-dotenv
```

Use compatible versions and pin them in `requirements.txt` after
testing.

**Deliverables**

-   Dataset download instructions.
-   Data dictionary.
-   Initial profiling report.
-   Reproducible local environment.

### Phase 2 -- S3 ingestion

Write a Python ingestion program that:

1.  Reads a configured list of source files.
2.  Checks that each file exists and is not empty.
3.  Records file size, row count and checksum where practical.
4.  Uploads the original file to the correct Bronze prefix.
5.  Writes an ingestion manifest.
6.  Logs success or failure.
7.  Avoids silently overwriting an earlier ingestion.

Suggested script responsibilities:

-   `extract.py`: locate and inspect input files.
-   `validate_source.py`: validate required columns and basic structure.
-   `upload_to_s3.py`: upload files using `boto3`.
-   `create_manifest.py`: record source, timestamp, checksum and status.

For repeatable ingestion, use an ingestion identifier or checksum.
Decide whether a re-run should skip an identical file or create a new
version.

**Deliverable:** Original files stored in S3 and an ingestion manifest
available for audit.

### Phase 3 -- Data cleaning and transformation

Start locally with PySpark if AWS costs or account setup are a concern.
Then move the same logic into an AWS Glue job.

Suggested transformations:

**Orders**

-   Parse order timestamps.
-   Normalize order status.
-   Validate order ID.
-   Identify duplicate order records.
-   Check timestamp ordering.

**Customers**

-   Validate customer identifiers.
-   Standardize state codes.
-   Handle missing location fields.
-   Check customer ID uniqueness.

**Products**

-   Validate product identifiers.
-   Handle missing category values.
-   Validate numeric dimensions and weight.
-   Join category translations.

**Order items**

-   Validate item sequence and order ID.
-   Check price and freight values.
-   Join product and seller references.

**Payments**

-   Validate payment type and amount.
-   Check installment count.
-   Preserve multiple payment records per order.

**Reviews**

-   Validate review score range.
-   Parse review timestamps.
-   Handle missing comments without dropping valid ratings.

For each dataset, record:

-   Input row count.
-   Output row count.
-   Invalid row count.
-   Duplicate count.
-   Null count for important fields.

Write invalid records to a quarantine zone with a reason code. Do not
silently discard them.

### Phase 4 -- Parquet and partitioning

Write cleaned datasets in Parquet format.

Why Parquet?

-   Columnar storage.
-   Efficient analytical reads.
-   Compression.
-   Better query performance for selected columns.

Consider partitioning large tables by a useful date field, such as
purchase year and month. Do not create excessive small partitions. For a
small dataset, demonstrate the design without creating hundreds of tiny
files.

Example:

``` text
silver/orders/year=2017/month=01/part-....parquet
gold/daily_sales/year=2017/month=01/part-....parquet
```

The partition column must match the actual date semantics used by the
table.

### Phase 5 -- AWS Glue ETL and Catalog

1.  Create an AWS Glue database.
2.  Configure the Glue job's IAM role with only the permissions it
    needs.
3.  Read Bronze data from S3.
4.  Apply schema and transformation logic.
5.  Write Silver and Gold outputs.
6.  Configure a Glue Crawler or define the table schema explicitly.
7.  Verify tables and columns in Glue Data Catalog.

Understand the difference:

-   **Glue job:** executes ETL transformations.
-   **Glue Crawler:** discovers data structure and updates catalog
    metadata.
-   **Glue Data Catalog:** stores metadata about databases, tables,
    schemas and locations.

A crawler does not clean the data or perform the ETL transformation.

### Phase 6 -- Athena analytics

Configure Athena to use the Glue Data Catalog and an S3 query-results
location.

Create SQL queries for:

1.  Monthly orders and revenue.
2.  Top 10 products by item revenue.
3.  Revenue by product category.
4.  Orders by customer state.
5.  Average order value by month.
6.  Payment-method distribution.
7.  Average delivery duration.
8.  Late-delivery percentage.
9.  Review-score distribution.
10. Repeat customer analysis.

Example query (adapt column names and table names to your actual
schema):

``` sql
SELECT
    date_trunc('month', purchase_timestamp) AS sales_month,
    COUNT(DISTINCT order_id) AS total_orders
FROM orders
GROUP BY 1
ORDER BY 1;
```

For revenue, use the correctly defined and appropriately aggregated fact
table.

### Phase 7 -- Data quality and reliability

Implement checks such as:

  Check                      Expected condition
  -------------------------- -------------------------------------
  Primary key uniqueness     No duplicate key at declared grain
  Required order ID          Not null
  Payment amount             Valid non-negative value
  Review score               Within allowed range
  Referential integrity      Referenced IDs exist where required
  Timestamp validity         Logical timestamp ordering
  Row-count reconciliation   Differences explained
  Schema validation          Required columns and types present

Add:

-   Python logging.
-   Structured error messages.
-   Unit tests using `pytest`.
-   A failure report.
-   A quarantine output.
-   Safe retry behavior.
-   Idempotent processing.

### Phase 8 -- Orchestration and monitoring

Once each step works independently:

-   Orchestrate ingestion and ETL using Glue Workflows or Step
    Functions.
-   Configure CloudWatch logs.
-   Monitor job failures and duration.
-   Create alarms for repeated failures.
-   Record pipeline run IDs and processing statistics.

Do not claim the pipeline is automated until you have actually
configured and tested the orchestration.

------------------------------------------------------------------------

## 8. Testing plan

### Unit tests

Test individual functions:

-   Date parsing.
-   Null handling.
-   Duplicate detection.
-   Type conversion.
-   Revenue calculation.
-   Partition path generation.

### Integration tests

Test:

-   Local file to S3 upload.
-   Glue job input and output.
-   Catalog table creation.
-   Athena query execution.
-   Quarantine behavior.

### Reconciliation tests

For every run, compare:

-   Source rows.
-   Accepted rows.
-   Quarantined rows.
-   Output rows.

Explain legitimate row-count changes caused by filtering, deduplication
or aggregation.

### Failure scenarios

Test at least these cases:

1.  Missing input file.
2.  Empty input file.
3.  Missing required column.
4.  Duplicate primary key.
5.  Invalid timestamp.
6.  Invalid numeric value.
7.  Missing foreign-key reference.
8.  Re-running the same ingestion.
9.  Permission failure.
10. Failed Glue job.

------------------------------------------------------------------------

## 9. Suggested repository structure

``` text
aws-ecommerce-data-lake-etl/
|
|-- README.md
|-- requirements.txt
|-- .gitignore
|-- .env.example
|
|-- config/
|   |-- dev.yaml
|
|-- data/
|   |-- sample/
|   |-- README.md
|
|-- ingestion/
|   |-- extract.py
|   |-- validate_source.py
|   |-- upload_to_s3.py
|   |-- create_manifest.py
|
|-- transformations/
|   |-- clean_orders.py
|   |-- clean_customers.py
|   |-- clean_products.py
|   |-- clean_payments.py
|   |-- build_gold_tables.py
|
|-- glue_jobs/
|   |-- ecommerce_etl.py
|
|-- data_quality/
|   |-- checks.py
|   |-- quarantine.py
|
|-- sql/
|   |-- sales_analysis.sql
|   |-- customer_analysis.sql
|   |-- product_analysis.sql
|   |-- delivery_analysis.sql
|
|-- tests/
|   |-- test_ingestion.py
|   |-- test_transformations.py
|   |-- test_data_quality.py
|
|-- docs/
|   |-- architecture.md
|   |-- data_dictionary.md
|   |-- data_model.md
|   |-- decisions.md
|
|-- infrastructure/
|   |-- iam/
|   |-- glue/
|   |-- terraform/
|
└-- screenshots/
```

------------------------------------------------------------------------

## 10. GitHub README requirements

Your repository README should contain:

1.  Project overview and business problem.
2.  Architecture diagram.
3.  Technology stack.
4.  Dataset source and download instructions.
5.  S3 folder layout.
6.  Data model and table grain.
7.  ETL workflow.
8.  Setup and execution instructions.
9.  SQL query examples and actual outputs.
10. Data-quality rules.
11. Testing results.
12. Screenshots of AWS resources.
13. Cost and cleanup instructions.
14. Limitations and future improvements.

Include only screenshots and results from your actual implementation.

------------------------------------------------------------------------

## 11. Definition of done

The project is ready to demonstrate when:

-   [ ] Dataset source and license are documented.
-   [ ] Data profiling is documented.
-   [ ] Raw data is stored in S3.
-   [ ] Ingestion manifest is generated.
-   [ ] PySpark transformations work.
-   [ ] Silver data is stored in Parquet.
-   [ ] Gold analytical tables are created.
-   [ ] Glue Data Catalog contains the tables.
-   [ ] Athena queries return verified results.
-   [ ] Data-quality checks are implemented.
-   [ ] Invalid data is quarantined.
-   [ ] Tests pass.
-   [ ] README and architecture diagram are complete.
-   [ ] AWS resources can be cleaned up safely.

------------------------------------------------------------------------

## 12. Interview preparation for this project

Be ready to explain:

-   Why did you choose S3 as the data lake?
-   What is the difference between a data lake and a data warehouse?
-   Why did you use Parquet instead of CSV?
-   What are Bronze, Silver and Gold layers?
-   How does AWS Glue work?
-   What is the Glue Data Catalog?
-   How does Athena query data in S3?
-   What is partition pruning?
-   What is the difference between a fact table and a dimension table?
-   What is the grain of your fact table?
-   How did you avoid double-counting payments and order items?
-   How do you handle schema changes?
-   How do you make the pipeline idempotent?
-   How do you handle invalid records?
-   How would you process data incrementally?
-   How would you optimize a slow Spark job?
-   How would you reduce Athena query costs?

You should be able to draw the architecture and explain one complete
record's journey from source CSV to an Athena query result.

------------------------------------------------------------------------

## 13. Cost and security checklist

-   Use a dedicated development bucket.
-   Enable appropriate S3 encryption.
-   Block public access.
-   Use IAM roles instead of embedding AWS keys.
-   Store secrets outside source control.
-   Configure a small development workload.
-   Avoid leaving Redshift clusters running.
-   Monitor Glue, Athena, S3 and KMS charges.
-   Configure an AWS Budget alert.
-   Delete test resources after use.
-   Keep any required evidence and screenshots before cleanup.

**Success criterion:** A reproducible, tested batch data pipeline with
traceable ingestion, validated transformations, analytical data models
and working SQL queries.
