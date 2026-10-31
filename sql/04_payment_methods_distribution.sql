-- ==============================================================================
-- Query 04: Payment Method Preference & Installment Dynamics
-- Business Question: Which payment methods are used most often and how are payments structured?
-- Table Used: ecommerce_lakehouse_db.fact_payments
-- ==============================================================================

SELECT
    payment_type,
    COUNT(DISTINCT order_id)            AS total_orders,
    COUNT(*)                            AS total_transactions,
    ROUND(SUM(payment_value), 2)        AS total_payment_value,
    ROUND(AVG(payment_value), 2)        AS avg_transaction_value,
    ROUND(AVG(payment_installments), 1) AS avg_installments,
    MAX(payment_installments)           AS max_installments,
    -- Share of total monetary volume processed
    ROUND(
        SUM(payment_value) * 100.0 / SUM(SUM(payment_value)) OVER (), 
        2
    ) AS payment_volume_share_pct
FROM ecommerce_lakehouse_db.fact_payments
GROUP BY payment_type
ORDER BY total_payment_value DESC;
