WITH depreciation_date AS (
    SELECT 
        pa.id,
        ca.calendar_at,
        ca.year,
        CASE 
            WHEN ca.year = DATE_PART('year', pa.payment_date + interval '10 years') 
            AND ca.month = DATE_PART('month', pa.payment_date)
            THEN 1 
            WHEN ca.month = 12 
            THEN 1
            ELSE 0 END AS flag_1_year,
        pa.amount/COUNT(*) OVER (PARTITION BY pa.id) AS monthly_depreciation
    FROM calendar AS ca
    CROSS JOIN payments AS pa
    WHERE 
        ca.calendar_at >= pa.payment_date AND ca.calendar_at < pa.payment_date + INTERVAL '10 years'
        AND pa.payment_type = 'equipment'
),
depreciation_sum AS (
    SELECT 
        *,  
        SUM(monthly_depreciation) OVER (PARTITION BY id ORDER BY calendar_at)AS accumulated_depreciation
    FROM depreciation_date
),
depreciation AS (
    SELECT
        year,
        SUM(accumulated_depreciation) AS total_amount
    FROM
        depreciation_sum 
    GROUP BY year   
),
ppe_purchase AS (
    SELECT
        DATE_PART('year', payment_date) AS year,
        SUM(amount) AS total_amount
    FROM payments 
    WHERE payment_type = 'equipment'
    GROUP BY DATE_PART('year', payment_date)
),
ppe_union AS (  
    SELECT * FROM depreciation
    UNION ALL
    SELECT * FROM ppe_purchase
)
    SELECT
        year,
        'Property, Plant & Equipment' AS account,
        SUM(total_amount) OVER (ORDER BY year)AS total_amount
    FROM ppe_union

