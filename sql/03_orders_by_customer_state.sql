-- ==============================================================================
-- Query 03: Geographic Distribution of Orders and Revenue by State
-- Business Question: Which states and cities contribute the most orders and gross revenue?
-- Tables Used: ecommerce_lakehouse_db.fact_order_items, ecommerce_lakehouse_db.dim_customers
-- ==============================================================================

SELECT
    c.customer_state,
    COUNT(DISTINCT f.order_id)           AS total_orders,
    COUNT(f.order_item_id)               AS total_items_sold,
    ROUND(SUM(f.price), 2)               AS total_product_revenue,
    ROUND(SUM(f.freight_value), 2)       AS total_freight_revenue,
    ROUND(SUM(f.total_item_value), 2)    AS total_gross_revenue,
    ROUND(SUM(f.price) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value,
    -- Cumulative Percentage distribution across states
    ROUND(
        COUNT(DISTINCT f.order_id) * 100.0 / SUM(COUNT(DISTINCT f.order_id)) OVER (), 
        2
    ) AS state_order_share_pct
FROM ecommerce_lakehouse_db.fact_order_items f
JOIN ecommerce_lakehouse_db.dim_customers c
    ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_orders DESC;
