-- ==============================================================================
-- Query 02: Top Product Categories by Item Revenue & Sales Volume
-- Business Question: Which categories and products generate the most revenue?
-- Tables Used: ecommerce_lakehouse_db.fact_order_items, ecommerce_lakehouse_db.dim_products
-- ==============================================================================

WITH category_sales AS (
    SELECT
        COALESCE(p.product_category_name_english, 'unclassified') AS category_name,
        COUNT(f.order_item_id)                                   AS units_sold,
        COUNT(DISTINCT f.order_id)                               AS unique_orders,
        COUNT(DISTINCT f.product_id)                             AS distinct_products,
        ROUND(SUM(f.price), 2)                                   AS total_item_revenue,
        ROUND(AVG(f.price), 2)                                   AS avg_unit_price
    FROM ecommerce_lakehouse_db.fact_order_items f
    JOIN ecommerce_lakehouse_db.dim_products p
        ON f.product_id = p.product_id
    WHERE f.order_status != 'canceled'
    GROUP BY 1
)
SELECT
    category_name,
    units_sold,
    unique_orders,
    distinct_products,
    total_item_revenue,
    avg_unit_price,
    -- Calculate contribution percentage of overall platform revenue
    ROUND(total_item_revenue / SUM(total_item_revenue) OVER () * 100, 2) AS revenue_share_pct,
    DENSE_RANK() OVER (ORDER BY total_item_revenue DESC) AS revenue_rank
FROM category_sales
ORDER BY revenue_rank ASC
LIMIT 15;
