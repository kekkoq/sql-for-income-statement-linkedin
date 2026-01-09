WITH pl_source AS (
    -- 1. REVENUE
    SELECT EXTRACT(YEAR FROM sale_at)::INT AS year, 'Revenue' AS account, SUM(quantity * price) AS amt 
    FROM sales GROUP BY 1
    UNION ALL

    -- 2a. CASH INFLOW (Revenue + 1 Month Lag for non-cash)
    SELECT 
        EXTRACT(YEAR FROM (CASE WHEN payment_method = 'cash' THEN sale_at ELSE sale_at + INTERVAL '1 month' END))::INT AS year,
        'Cash' AS account, 
        SUM(quantity * price) AS amt
    FROM sales 
    GROUP BY 1, 2
    UNION ALL

    -- 2b. CASH INFLOW: LOANS (Initial Principal Received)
    SELECT 
        EXTRACT(YEAR FROM loan_at)::INT AS year, 
        'Cash' AS account, 
        SUM(value) AS amt 
    FROM loans 
    GROUP BY 1
    UNION ALL

    -- 3. CASH OUTFLOW: PURCHASES (Inventory + 1 Month Lag for non-cash)
    SELECT 
        EXTRACT(
            YEAR FROM (CASE WHEN payment_method = 'cash' THEN purchase_at ELSE purchase_at + INTERVAL '1 month' END)
        )::INT AS year,
        'Cash' AS account, -SUM(quantity * amount) AS amt
    FROM purchases 
    GROUP BY 1, 2
    UNION ALL

    -- 4. CASH OUTFLOW: OPERATING EXPENSES (Payments table)
    -- This includes equipment, loan principal, and all operating costs
    SELECT EXTRACT(YEAR FROM payment_date)::INT AS year, 
    'Cash' AS account, 
    -SUM(amount) AS amt
    FROM payments 
    WHERE payment_type IN ('equipment', 'interest', 'wage', 'utility', 'tax', 'loan', 'rent')
    GROUP BY 1
    UNION ALL

    -- 2. COGS (P&L Impact)
    SELECT EXTRACT(YEAR FROM s.sale_at)::INT AS year, 'COGS' AS account, -SUM(pc.amount * s.quantity) 
    FROM sales s 
    LEFT JOIN (SELECT DISTINCT product_name, amount FROM purchases) pc ON s.product_name = pc.product_name 
    GROUP BY 1
    
    UNION ALL
    -- 3. DEPRECIATION EXPENSE (P&L Impact)
    SELECT year, 'Depr_Exp' AS account, -SUM(annual_depreciation_expense) AS amt
    FROM equipment_depreciation_schedule GROUP BY 1
    
    UNION ALL
    -- 4. PPE SNAPSHOT (Balance Sheet Anchor - Flat 31k)
    SELECT year, 'PPE_Snapshot' AS account, SUM(gross_val) AS amt
    FROM equipment_depreciation_schedule GROUP BY 1

    UNION ALL
    -- 5. ACCOUNTS RECEIVABLE (Year-end unpaid sales)
    SELECT EXTRACT(YEAR FROM payment_at)::INT AS year, 'Accounts_Receivable' AS account, SUM(price * quantity) AS amt
    FROM sales WHERE payment_method <> 'cash' AND DATE_PART('month', payment_at) = 12 GROUP BY 1

    UNION ALL
    -- 6. INVENTORY: Additions (Purchases)
    SELECT EXTRACT(YEAR FROM purchase_at)::INT AS year, 'Inventory' AS account, SUM(amount * quantity) AS amt
    FROM purchases GROUP BY 1
    UNION ALL
    -- 7. INVENTORY: Deductions (Sales at Cost)
    SELECT EXTRACT(YEAR FROM sale_at)::INT AS year, 'Inventory' AS account, -SUM(s.quantity * pc.amount) AS amt
    FROM sales AS s
    LEFT JOIN (SELECT DISTINCT product_name, amount FROM purchases) AS pc ON s.product_name = pc.product_name
    GROUP BY 1

    UNION ALL
    -- 8. LOAN BALANCE (Liability)
    -- Initial Loan Value (Increases Liability)
    SELECT EXTRACT(YEAR FROM loan_at)::INT AS year, 'Loan_Principal' AS account, SUM(value) AS amt 
    FROM loans GROUP BY 1
    UNION ALL
    -- Principal Payments (Decreases Liability)
    SELECT EXTRACT(YEAR FROM payment_date)::INT AS year, 'Loan_Principal' AS account, -SUM(amount) AS amt 
    FROM payments WHERE payment_type = 'loan' GROUP BY 1

    UNION ALL
    -- 8. OPERATIONAL EXPENSES (P&L Accrual - the month incurred)
    SELECT 
        EXTRACT(YEAR FROM accrual_date)::INT AS year, 
        account, 
        -SUM(amount) AS amt
    FROM expense_accrual_schedule GROUP BY 1, 2

    UNION ALL
    -- 9. ACCOUNTS PAYABLE: Increase (When Accrued)
    SELECT EXTRACT(YEAR FROM accrual_date)::INT AS year, 
    'Accounts_Payable' AS account, 
    SUM(amount) AS amt
    FROM expense_accrual_schedule 
    GROUP BY 1, 2
    UNION ALL

    -- 10. ACCOUNTS PAYABLE: Decrease (When Paid)
    SELECT EXTRACT(YEAR FROM cash_payment_date)::INT AS year, 
    'Accounts_Payable' AS account, 
    -SUM(amount) AS amt
    FROM expense_accrual_schedule 
    GROUP BY 1, 2
),
yearly_summaries AS (
    SELECT 
        year,
        account,
        SUM(amt) AS annual_movement,
        SUM(SUM(amt)) OVER (PARTITION BY account ORDER BY year) AS running_balance
    FROM pl_source
    GROUP BY 1, 2
)
SELECT 
    year,
    -- P&L: Includes Revenue, COGS, Depreciation, and Operating Accounts
    ROUND(COALESCE(MAX(annual_movement) FILTER (WHERE account = 'Revenue'), 0)::numeric, 2) AS revenue,
    ROUND(SUM(annual_movement) FILTER (
        WHERE account IN ('Revenue', 'COGS', 'Depr_Exp', 'interest', 'wage', 'tax', 'rent', 'utility')
    )::numeric, 2) AS net_income,
    
    -- BALANCE SHEET: Current Assets & Liabilities
    ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Cash'), 0)::numeric, 2) AS cash_bal,
    ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Accounts_Receivable'), 0)::numeric, 2) AS accounts_receivable,
    ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Accounts_Payable'), 0)::numeric, 2) AS accounts_payable,

    -- Long-Term Liabilities
    ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Loan_Principal'), 0)::numeric, 2) AS debt_remaining,
    
    -- CURRENT ASSETS
    ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Inventory'), 0)::numeric, 2) AS inventory_bal,

    -- FIXED ASSETS: Net PPE calculation
    ROUND(COALESCE(MAX(annual_movement) FILTER (WHERE account = 'PPE_Snapshot'), 0)::numeric, 2) AS gross_ppe,
    ROUND(GREATEST(0, 
        COALESCE(MAX(annual_movement) FILTER (WHERE account = 'PPE_Snapshot'), 0) + 
        COALESCE(MAX(running_balance) FILTER (WHERE account = 'Depr_Exp'), 0)
    )::numeric, 2) AS net_ppe
FROM yearly_summaries
WHERE year <= 2031
GROUP BY year ORDER BY year;