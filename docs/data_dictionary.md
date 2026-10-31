# E-Commerce Data Lake: Data Dictionary & Modeling Specifications

This document defines the schema, data types, primary keys, relationships, and grains for all source and curated datasets in the platform.

---

## 1. Entity Relationship & Grain Summary

| Dataset / Table | Declared Grain (What 1 row represents) | Primary Key | Foreign Keys |
| :--- | :--- | :--- | :--- |
| `orders` | One distinct purchase transaction | `order_id` | `customer_id` $\rightarrow$ `customers.customer_id` |
| `order_items` | One item line inside an order | `order_id` + `order_item_id` | `order_id` $\rightarrow$ `orders.order_id`<br>`product_id` $\rightarrow$ `products.product_id`<br>`seller_id` $\rightarrow$ `sellers.seller_id` |
| `payments` | One payment transaction/installment record | `order_id` + `payment_sequential` | `order_id` $\rightarrow$ `orders.order_id` |
| `customers` | One unique customer order identity | `customer_id` | N/A (`customer_unique_id` tracks repeat users) |
| `products` | One distinct product catalog item | `product_id` | `product_category_name` $\rightarrow$ `category_translation` |
| `sellers` | One registered merchant / seller | `seller_id` | N/A |
| `reviews` | One customer review score & comment | `review_id` | `order_id` $\rightarrow$ `orders.order_id` |
| `category_translation` | Portuguese to English category map | `product_category_name` | N/A |

> [!WARNING]
> **Modeling Warning — Many-to-Many Fan-Out:**
> Never perform a direct Cartesian join between `order_items` and `payments` on `order_id` and subsequently compute `SUM(price)` or `SUM(payment_value)`. An order with 3 items and 2 payments will produce 6 joined rows ($3 \times 2$), multiplying price totals by 2 and payment totals by 3. Always aggregate at the `order_id` level first before joining!

---

## 2. Table Schemas

### Table: `orders`
- **Source File:** `olist_orders_dataset.csv`
- **Silver Table:** `orders` (Partitioned by `year`, `month` on `order_purchase_timestamp`)

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | String | **No** | Unique 32-character hexadecimal order identifier. |
| `customer_id` | String | String | **No** | Key to the customer record for this specific transaction. |
| `order_status` | String | String | **No** | Order lifecycle state (`delivered`, `shipped`, `canceled`, etc.). |
| `order_purchase_timestamp` | String | Timestamp (UTC) | **No** | Timestamp when the customer placed the order. |
| `order_approved_at` | String | Timestamp (UTC) | Yes | Payment approval timestamp. Null if payment failed or canceled early. |
| `order_delivered_carrier_date` | String | Timestamp (UTC) | Yes | Carrier fulfillment handoff timestamp. Null if canceled prior to dispatch. |
| `order_delivered_customer_date` | String | Timestamp (UTC) | Yes | Actual customer delivery timestamp. Used for delivery KPI calculations. |
| `order_estimated_delivery_date` | String | Timestamp (UTC) | **No** | Estimated delivery date communicated to customer at purchase. |

---

### Table: `order_items`
- **Source File:** `olist_order_items_dataset.csv`
- **Silver Table:** `order_items`

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | String | **No** | References `orders.order_id`. |
| `order_item_id` | String / Int | Integer | **No** | Sequential line item number within the order (1, 2, 3...). |
| `product_id` | String | String | **No** | References `products.product_id`. |
| `seller_id` | String | String | **No** | References `sellers.seller_id`. |
| `shipping_limit_date` | String | Timestamp (UTC) | **No** | Seller shipping deadline date to hand over to carrier. |
| `price` | String / Float | Double | **No** | Item price in BRL (excluding freight). Must be $\ge 0$. |
| `freight_value` | String / Float | Double | **No** | Shipping freight cost allocated to this item in BRL. Must be $\ge 0$. |

---

### Table: `payments`
- **Source File:** `olist_order_payments_dataset.csv`
- **Silver Table:** `payments`

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | String | **No** | References `orders.order_id`. |
| `payment_sequential` | String / Int | Integer | **No** | Sequence sequence of payment (allows split payments, e.g., voucher + card). |
| `payment_type` | String | String | **No** | Method: `credit_card`, `boleto`, `voucher`, `debit_card`. |
| `payment_installments` | String / Int | Integer | **No** | Number of credit card installments chosen by customer. $\ge 1$. |
| `payment_value` | String / Float | Double | **No** | Amount charged in BRL. Negative values are quarantined. |

---

### Table: `customers`
- **Source File:** `olist_customers_dataset.csv`
- **Silver Table:** `customers`

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | String | String | **No** | Transaction-specific identifier (1-to-1 with `orders`). |
| `customer_unique_id` | String | String | **No** | Real customer identifier across multiple historical purchases. |
| `customer_zip_code_prefix` | String | String | **No** | 5-digit Brazilian postal code prefix. Standardized with leading zeros. |
| `customer_city` | String | String | **No** | City name normalized to lowercase title. |
| `customer_state` | String | String (CHAR 2) | **No** | 2-letter Brazilian state code (e.g., `SP`, `RJ`, `MG`). |

---

### Table: `products`
- **Source File:** `olist_products_dataset.csv`
- **Silver Table:** `products` (Enriched with English translation)

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | String | String | **No** | Unique product identifier. |
| `product_category_name` | String | String | Yes | Original Portuguese category name. |
| `product_category_name_english` | String | String | Yes | English category name derived via lookup translation table. |
| `product_name_lenght` | String / Int | Integer | Yes | Length of product title in characters. |
| `product_description_lenght` | String / Int | Integer | Yes | Length of product description in characters. |
| `product_photos_qty` | String / Int | Integer | Yes | Number of product photos published on listing. |
| `product_weight_g` | String / Float | Double | Yes | Product weight in grams. Must be $> 0$. |
| `product_length_cm` | String / Float | Double | Yes | Package length in centimeters. |
| `product_height_cm` | String / Float | Double | Yes | Package height in centimeters. |
| `product_width_cm` | String / Float | Double | Yes | Package width in centimeters. |

---

### Table: `sellers`
- **Source File:** `olist_sellers_dataset.csv`
- **Silver Table:** `sellers`

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `seller_id` | String | String | **No** | Unique merchant identifier. |
| `seller_zip_code_prefix` | String | String | **No** | 5-digit Brazilian postal code prefix. |
| `seller_city` | String | String | **No** | Merchant operating city. |
| `seller_state` | String | String (CHAR 2) | **No** | 2-letter Brazilian merchant state code. |

---

### Table: `reviews`
- **Source File:** `olist_order_reviews_dataset.csv`
- **Silver Table:** `reviews`

| Column Name | Source Type | Silver Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `review_id` | String | String | **No** | Unique review identifier. |
| `order_id` | String | String | **No** | References `orders.order_id`. |
| `review_score` | String / Int | Integer | **No** | Satisfaction rating on scale from 1 (poor) to 5 (excellent). |
| `review_comment_title` | String | String | Yes | Customer submitted review heading. |
| `review_comment_message` | String | String | Yes | Detailed customer review feedback text. |
| `review_creation_date` | String | Timestamp (UTC) | **No** | Date satisfaction survey was sent to customer. |
| `review_answer_timestamp` | String | Timestamp (UTC) | **No** | Timestamp when customer completed the survey. |

---

## 3. Curated Gold Marts & Grain Definitions

1. **`gold_daily_sales`**:
   - **Grain:** One row per calendar date (`order_date`).
   - **Metrics:** `total_orders`, `total_items_sold`, `item_revenue`, `freight_revenue`, `total_gross_revenue`, `average_order_value`.
2. **`gold_delivery_kpis`**:
   - **Grain:** One row per customer state (`customer_state`) and month (`order_month`).
   - **Metrics:** `total_delivered_orders`, `late_delivery_count`, `late_delivery_rate`, `avg_delivery_duration_days`.
3. **`gold_product_performance`**:
   - **Grain:** One row per product category (`product_category_name_english`).
   - **Metrics:** `units_sold`, `total_revenue`, `avg_item_price`, `avg_review_score`.
