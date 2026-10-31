-- ==============================================================================
-- Query 01: Monthly Order and Revenue Trend & MoM Growth
-- Business Question: What is the monthly order and revenue trend, and how is AOV evolving?
-- Table Used: ecommerce_lakehouse_db.gold_daily_sales
-- ==============================================================================

WITH monthly_sales AS (
    SELECT
        date_trunc('month', order_date) AS sales_month,
        SUM(total_orders)               AS total_orders,
        SUM(total_items_sold)           AS total_items_sold,
        ROUND(SUM(item_revenue), 2)     AS total_item_revenue,
        ROUND(SUM(freight_revenue), 2)  AS total_freight_revenue,
        ROUND(SUM(gross_revenue), 2)    AS total_gross_revenue,
        ROUND(SUM(item_revenue) / SUM(total_orders), 2) AS average_order_value
    FROM ecommerce_lakehouse_db.gold_daily_sales
    GROUP BY 1
)
SELECT
    sales_month,
    total_orders,
    total_items_sold,
    total_item_revenue,
    total_freight_revenue,
    total_gross_revenue,
    average_order_value,
    -- Month-over-Month Revenue Growth Percentage
    ROUND(
        (total_gross_revenue - LAG(total_gross_revenue, 1) OVER (ORDER BY sales_month)) 
        / NULLIF(LAG(total_gross_revenue, 1) OVER (ORDER BY sales_month), 0) * 100, 
        2
    ) AS mom_revenue_growth_pct
FROM monthly_sales
ORDER BY sales_month ASC;
