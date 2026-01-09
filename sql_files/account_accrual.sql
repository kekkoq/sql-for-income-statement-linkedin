DROP TABLE IF EXISTS expense_accrual_schedule;

CREATE TABLE expense_accrual_schedule AS
SELECT 
    id,
    payment_type AS account,
    amount,
    -- The P&L Date: The month PRIOR to the actual payment
    DATE_TRUNC('month', payment_date - INTERVAL '1 month')::DATE AS accrual_date,
    -- The Cash Date: When it actually left the bank
    payment_date AS cash_payment_date
FROM payments
WHERE 
    payment_type IN ('wage', 'utility', 'tax')
    AND EXTRACT(YEAR FROM payment_date) IN (2021, 2022, 2023);  