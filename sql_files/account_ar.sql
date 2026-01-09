CREATE MATERIALIZED VIEW account_ar AS
SELECT 
    DATE_PART('year', payment_at) AS year,
    'Accounts_Receivable' AS accont,
    SUM(price * quantity) AS total_amount
FROM sales
WHERE payment_method <> 'cash' AND DATE_PART('month', payment_at) = 12
GROUP BY DATE_PART('year', payment_at)      