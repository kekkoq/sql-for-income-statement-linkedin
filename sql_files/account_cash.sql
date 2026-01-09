CREATE MATERIALIZED VIEW account_cash AS
With purchase_dates AS (
    SELECT
        CASE
            WHEN payment_method = 'cash' THEN payment_at 
            ELSE payment_at + INTERVAL '1 month'
        END AS actual_payment_date,
        -SUM(quantity*amount) AS purchase_amount
    FROM
        purchases
    GROUP BY
        CASE
            WHEN payment_method = 'cash' THEN payment_at 
            ELSE payment_at + INTERVAL '1 month'
        END
),
purchase AS (
    SELECT
        DATE_PART('year', actual_payment_date) AS year,
        SUM(purchase_amount) AS total_amount
    FROM
        purchase_dates
    GROUP BY
        DATE_PART('year', actual_payment_date)
),
revenue_dates AS (
    SELECT
        CASE
            WHEN payment_method = 'cash' THEN payment_at 
            ELSE payment_at + INTERVAL '1 month'
        END AS actual_payment_date,
        SUM(quantity*price) AS sales_amount
    FROM
        sales
    GROUP BY
        CASE
            WHEN payment_method = 'cash' THEN payment_at 
            ELSE payment_at + INTERVAL '1 month'
        END
),
revenue AS (
    SELECT
        DATE_PART('year', actual_payment_date) AS year,
        SUM(sales_amount) AS total_amount
    FROM
        revenue_dates
    GROUP BY
        DATE_PART('year', actual_payment_date)
),
loan_in AS (
    SELECT
        DATE_PART('year', loan_at) AS year,
        SUM(value) AS total_amount
    FROM
        loans
     GROUP BY
        DATE_PART('year', loan_at)
),
expense AS (
    SELECT
        DATE_PART('year', payment_date) AS year,
        -SUM(amount) AS total_amount
    FROM
        payments
    WHERE PAYMENT_TYPE in ('equipment', 'interest', 'wage', 'utility', 'tax', 'loan', 'rent')
     GROUP BY
        DATE_PART('year', payment_date )
),
cash_union AS (
    SELECT * FROM purchase
    UNION ALL
    SELECT * FROM revenue
    UNION ALL
    SELECT * FROM loan_in
    UNION ALL
    SELECT * FROM expense
),
cash_amount AS (
    SELECT
        year,
        SUM(total_amount) AS total_amount
    FROM
        cash_union
    GROUP BY
        year
)
SELECT 
    year,
    'cash' AS account_type,
    SUM(total_amount) OVER (ORDER BY year) AS total_amount
    FROM cash_amount

   