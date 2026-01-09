WITH combined_data AS (
    -- Collect all inflows (Loans)
    SELECT 
        EXTRACT(YEAR FROM loan_at)::INT AS year, 
        value AS amount
    FROM loans
    
    UNION ALL
    
    -- Collect all outflows (Payments)
    SELECT 
        EXTRACT(YEAR FROM payment_date)::INT AS year, 
        -amount AS amount
    FROM payments
    WHERE payment_type = 'loan'
)
SELECT
    year,
    'Loan' AS account,
    -- Sum the yearly amounts and then calculate the running total
    SUM(SUM(amount)) OVER (ORDER BY year) AS cumulative_loan_amount
FROM 
    combined_data
GROUP BY 
    year
ORDER BY 
    year;