-- ==============================================================================
-- Query 05: Delivery SLA Performance and Late Delivery Rates by State
-- Business Question: How frequently are orders delivered late and what is the transit latency?
-- Table Used: ecommerce_lakehouse_db.gold_delivery_kpis
-- ==============================================================================

SELECT
    customer_state,
    SUM(total_delivered_orders)        AS total_delivered_orders,
    SUM(late_orders_count)             AS total_late_orders,
    ROUND(
        SUM(late_orders_count) * 100.0 / NULLIF(SUM(total_delivered_orders), 0), 
        2
    ) AS overall_late_delivery_rate_pct,
    ROUND(AVG(avg_delivery_duration_days), 2) AS avg_delivery_duration_days
FROM ecommerce_lakehouse_db.gold_delivery_kpis
GROUP BY customer_state
ORDER BY overall_late_delivery_rate_pct DESC, total_delivered_orders DESC;
