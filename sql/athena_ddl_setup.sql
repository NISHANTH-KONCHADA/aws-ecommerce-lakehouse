-- ==============================================================================
-- Amazon Athena External Table DDL Setup
-- Database: ecommerce_lakehouse_db
-- Storage: Apache Parquet (Snappy Compressed) on Amazon S3
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_lakehouse_db
COMMENT 'Central Data Lakehouse for E-Commerce Analytics'
LOCATION 's3://YOUR_BUCKET_NAME/';

-- ------------------------------------------------------------------------------
-- 1. Silver Orders (Partitioned by Year and Month)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.silver_orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/silver/orders/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Repair partitions after new data is loaded
MSCK REPAIR TABLE ecommerce_lakehouse_db.silver_orders;

-- ------------------------------------------------------------------------------
-- 2. Silver Order Items
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.silver_order_items (
    order_id STRING,
    order_item_id INT,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price DOUBLE,
    freight_value DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/silver/order_items/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ------------------------------------------------------------------------------
-- 3. Silver Payments
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.silver_payments (
    order_id STRING,
    payment_sequential INT,
    payment_type STRING,
    payment_installments INT,
    payment_value DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/silver/payments/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ------------------------------------------------------------------------------
-- 4. Silver Customers
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.silver_customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/silver/customers/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ------------------------------------------------------------------------------
-- 5. Gold Daily Sales Mart
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.gold_daily_sales (
    order_date DATE,
    total_orders BIGINT,
    total_items_sold BIGINT,
    item_revenue DOUBLE,
    freight_revenue DOUBLE,
    gross_revenue DOUBLE,
    average_order_value DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/gold/gold_daily_sales/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ------------------------------------------------------------------------------
-- 6. Gold Delivery KPIs Mart
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_lakehouse_db.gold_delivery_kpis (
    customer_state STRING,
    order_month STRING,
    total_delivered_orders BIGINT,
    late_orders_count BIGINT,
    late_delivery_rate_pct DOUBLE,
    avg_delivery_duration_days DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/gold/gold_delivery_kpis/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
