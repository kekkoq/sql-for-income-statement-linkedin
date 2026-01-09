WITH ppe_events AS (
    -- 1. PURCHASE: Add the full value as a positive number
    SELECT 
        EXTRACT(YEAR FROM payment_date)::INT AS year,
        amount AS amount_change
    FROM payments 
    WHERE payment_type = 'equipment'

    UNION ALL

    -- 2. DEPRECIATION: Subtract the value as negative numbers
    -- Using the 10-year Half-Year Convention
    SELECT 
        EXTRACT(YEAR FROM payment_date)::INT + i AS year,
        CASE 
            WHEN i = 0 THEN -(amount / 10.0) * 0.5   -- Year 1 (Half)
            WHEN i = 10 THEN -(amount / 10.0) * 0.5  -- Year 11 (Half)
            ELSE -(amount / 10.0)                    -- Years 2-10 (Full)
        END AS amount_change
    FROM payments,
         generate_series(0, 10) AS i
    WHERE payment_type = 'equipment'
)
SELECT
    year,
    'Property, Plant & Equipment' AS account,
    -- This calculates (Purchases - Depreciation) cumulatively over time
    ROUND(SUM(SUM(amount_change)) OVER (ORDER BY year)::numeric, 2) AS net_ppe_balance
FROM 
    ppe_events
GROUP BY 
    year
ORDER BY 
    year;