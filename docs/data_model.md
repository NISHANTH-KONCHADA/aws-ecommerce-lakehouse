# Dimensional Data Model & Star Schema Architecture

## 1. Dimensional Model Overview

The Gold zone implements a Kimball-style Dimensional Model optimized for Online Analytical Processing (OLAP) queries via Amazon Athena and BI tools (QuickSight, Tableau, PowerBI).

```text
               ┌────────────────┐
               │    dim_date    │
               │ (date_key PK)  │
               └───────┬────────┘
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
┌──────────────────┐        ┌──────────────────┐
│ fact_order_items │        │  fact_payments   │
│ (grain: item)    │        │  (grain: pymt)   │
└────────┬─────────┘        └────────┬─────────┘
         │                           │
 ┌───────┴───────┬──────────────┐    │
 │               │              │    │
 ▼               ▼              ▼    ▼
┌───────────┐ ┌───────────┐  ┌───────────────┐
│dim_product│ │dim_sellers│  │ dim_customers │
└───────────┘ └───────────┘  └───────────────┘
```

---

## 2. Table Specifications

### Fact Tables

1. **`fact_order_items`**:
   - **Grain:** One row per individual item line inside an order.
   - **Primary Key:** `order_id` + `order_item_id`.
   - **Foreign Keys:** `order_date_key` $\rightarrow$ `dim_date.date_key`, `customer_id` $\rightarrow$ `dim_customers.customer_id`, `product_id` $\rightarrow$ `dim_products.product_id`, `seller_id` $\rightarrow$ `dim_sellers.seller_id`.
   - **Degenerate Dimension:** `order_status`.
   - **Measures (Additive):** `price`, `freight_value`, `total_item_value`.

2. **`fact_payments`**:
   - **Grain:** One row per payment transaction or installment.
   - **Primary Key:** `order_id` + `payment_sequential`.
   - **Foreign Keys:** `order_date_key` $\rightarrow$ `dim_date.date_key`, `customer_id` $\rightarrow$ `dim_customers.customer_id`.
   - **Degenerate Dimensions:** `payment_type`, `order_status`.
   - **Measures (Additive):** `payment_value`, `payment_installments`.

### Dimension Tables

1. **`dim_customers`**:
   - **Grain:** One row per distinct `customer_id`.
   - **Attributes:** `customer_unique_id` (tracks repeat human buyers), `customer_city`, `customer_state`, `customer_zip_code_prefix`.

2. **`dim_products`**:
   - **Grain:** One row per catalog `product_id`.
   - **Attributes:** `product_category_name`, `product_category_name_english`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`.

3. **`dim_sellers`**:
   - **Grain:** One row per merchant `seller_id`.
   - **Attributes:** `seller_city`, `seller_state`, `seller_zip_code_prefix`.

4. **`dim_date`**:
   - **Grain:** One row per calendar day.
   - **Attributes:** `date_key` (integer YYYYMMDD), `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`, `day_name`, `is_weekend`.

---

## 3. Curated Aggregated Marts

1. **`gold_daily_sales`**:
   - Pre-aggregated daily revenue, order velocity, item counts, and average order values (AOV).
2. **`gold_product_performance`**:
   - Pre-aggregated category sales volume, revenue contribution, and unique active merchant counts.
3. **`gold_delivery_kpis`**:
   - State-level logistics performance tracking delivered order volume, late delivery counts, late delivery rate percentage, and average transit time in days.
