WITH revenue AS (
    SELECT 
        EXTRACT(YEAR FROM sale_at):: INT AS year,
        'Revenue' AS transaction_type,
        1 AS order_process,
        SUM(quantity*price) AS total_amount
    FROM sales
    GROUP BY EXTRACT(YEAR FROM sale_at):: INT
),
product_costS AS (
    SELECT 
       DISTINCT product_name,
       amount
    FROM purchases
),
Cogs AS (
    SELECT 
        EXTRACT(YEAR FROM s.sale_at):: INT AS year,
        'Cost of Goods Sold' AS transaction_type,
        2 AS order_process,
        -SUM(pc.amount * s.quantity) AS total_amount
    FROM sales AS s
    LEFT JOIN product_costs AS pc ON s.product_name = pc.product_name
    GROUP BY EXTRACT(YEAR FROM s.sale_at):: INT
),
depreciation_date AS (
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
        -SUM(monthly_depreciation) OVER (PARTITION BY id ORDER BY calendar_at)AS cumulative_depreciation
    FROM depreciation_date
),
depreciation AS (
    SELECT
        year,
        'Depreciation' AS transaction_type,
        3 AS order_process,
        -SUM(cumulative_depreciation) AS total_amount
    FROM
        depreciation_sum 
    GROUP BY year   
),
expense AS (
    SELECT
        DATE_PART('year', payment_date) AS year,
        CASE 
             WHEN PAYMENT_TYPE = 'interest' THEN 'Interest Expense'
             WHEN PAYMENT_TYPE = 'wage' THEN 'Wage Expense'
             WHEN PAYMENT_TYPE = 'tax' THEN 'Tax Expense'
             WHEN PAYMENT_TYPE in ('rent', 'utility') THEN 'Operational Expense' END AS transaction_type,
        CASE WHEN PAYMENT_TYPE = 'interest' THEN 4
             WHEN PAYMENT_TYPE = 'wage' THEN 5
             WHEN PAYMENT_TYPE = 'tax' THEN 6
             WHEN PAYMENT_TYPE in ('rent', 'utility') THEN 7 END AS order_process,
        -SUM(amount) AS total_amount
    FROM
        payments
    WHERE PAYMENT_TYPE in ('interest', 'wage', 'utility', 'tax', 'rent')
    GROUP BY
        DATE_PART('year', payment_date ), 
        CASE 
             WHEN PAYMENT_TYPE = 'interest' THEN 'Interest Expense'
             WHEN PAYMENT_TYPE = 'wage' THEN 'Wage Expense'
             WHEN PAYMENT_TYPE = 'tax' THEN 'Tax Expense'
             WHEN PAYMENT_TYPE in ('rent', 'utility') THEN 'Operational Expense' END,
        CASE WHEN PAYMENT_TYPE = 'interest' THEN 4
             WHEN PAYMENT_TYPE = 'wage' THEN 5
             WHEN PAYMENT_TYPE = 'tax' THEN 6
             WHEN PAYMENT_TYPE in ('rent', 'utility') THEN 7 END 
),
re_union AS (
    SELECT * FROM revenue
    UNION ALL
    SELECT * FROM cogs
    UNION ALL
    SELECT * FROM depreciation
    UNION ALL
    SELECT * FROM expense
    UNION ALL
    SELECT 
        DISTINCT(EXTRACT(YEAR FROM calendar_at)) AS year,
        'RE - Beginning Balance' AS transaction_type,
        0 AS order_process,
        0 AS total_amount
    FROM calendar
    UNION ALL
     SELECT 
        DISTINCT(EXTRACT(YEAR FROM calendar_at)) AS year,
        'RE - Ending Balance' AS transaction_type,
        999 AS order_process,
        0 AS total_amount
    FROM calendar
),
re_details AS (
    SELECT 
        year,
        transaction_type,
        total_amount,
        SUM(total_amount) OVER (PARTITION BY year ORDER BY order_process ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
    FROM re_union
    GROUP BY 
        year,
        transaction_type,
        order_process
)
SELECT * FROM re_details