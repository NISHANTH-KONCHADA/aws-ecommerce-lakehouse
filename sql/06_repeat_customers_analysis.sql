-- ==============================================================================
-- Query 06: Repeat Customer Retention and Lifetime Contribution
-- Business Question: How many customers make repeat purchases vs one-time purchases?
-- Tables Used: ecommerce_lakehouse_db.silver_orders, ecommerce_lakehouse_db.silver_customers
-- ==============================================================================

WITH customer_order_frequencies AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS orders_placed,
        MIN(o.order_purchase_timestamp) AS first_order_date,
        MAX(o.order_purchase_timestamp) AS latest_order_date
    FROM ecommerce_lakehouse_db.silver_orders o
    JOIN ecommerce_lakehouse_db.silver_customers c
        ON o.customer_id = c.customer_id
    WHERE o.order_status != 'canceled'
    GROUP BY c.customer_unique_id
),
retention_buckets AS (
    SELECT
        customer_unique_id,
        orders_placed,
        CASE
            WHEN orders_placed = 1 THEN '1_One-Time Buyer'
            WHEN orders_placed = 2 THEN '2_Two Purchases'
            ELSE '3_Frequent Repeat Buyer (3+)'
        END AS buyer_cohort
    FROM customer_order_frequencies
)
SELECT
    buyer_cohort,
    COUNT(customer_unique_id) AS total_customers,
    ROUND(
        COUNT(customer_unique_id) * 100.0 / SUM(COUNT(customer_unique_id)) OVER (), 
        2
    ) AS customer_share_pct
FROM retention_buckets
GROUP BY buyer_cohort
ORDER BY buyer_cohort ASC;
