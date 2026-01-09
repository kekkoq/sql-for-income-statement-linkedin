
from utils import get_engine, logger
from sqlalchemy import text 
import os

def extract_load():
    """Reads the SQL setup file and executes it in the Docker DB."""
    engine = get_engine()
    # The path to your SQL script inside the container
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(curr_dir, '..', 'data', 'setup-postgresql.sql')

    # 1. DROP EXISTING TABLES FIRST
    cleanup_sql = """
    DROP TABLE IF EXISTS loans CASCADE;
    DROP TABLE IF EXISTS sales CASCADE;
    DROP TABLE IF EXISTS purchases CASCADE;
    DROP TABLE IF EXISTS payments CASCADE;
    DROP TABLE IF EXISTS shifts CASCADE;
    DROP TABLE IF EXISTS equipment_depreciation_schedule CASCADE;
    DROP TABLE IF EXISTS expense_accrual_schedule CASCADE;
    """

    with engine.connect() as conn:
        conn.execute(text(cleanup_sql))
        conn.commit()
        logger.info("Database cleaned.")

    # 2. Read the big file
    if not os.path.exists(sql_file_path):
        logger.error(f"SQL file not found at path: {sql_file_path}")
        return

    with open(sql_file_path, 'r') as file:
        commands = file.read().split(';')

    # 3. Atomic loop
    logger.info(f"Loading {len(commands)} commands. This might take a minute...")
    for i, command in enumerate(commands):
        try:
            with engine.connect() as conn:
                conn.execute(text(command))
                conn.commit()
        except Exception as e:
            # log the error but DO NOT 'raise'. must finish the load.
            if "does not exist" not in str(e) and "already exists" not in str(e):
                logger.warning(f"Skipped command {i} due to error: {str(e)[:50]}...")
            continue

    logger.info("DONE! Financial data should now be ready for analysis.")

def create_custom_financial_tables():
    """Creates custom financial tables needed for analysis."""
    engine = get_engine()
    with engine.connect() as conn:
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS calendar (calendar_at DATE);
                TRUNCATE TABLE calendar;
                INSERT INTO calendar (calendar_at) 
                SELECT generate_series('2020-01-01'::date, '2025-12-31'::date, '1 year'::interval);
            """))
            # Drop existing tables if they exist
            conn.execute(text("CREATE TABLE IF NOT EXISTS equipment_depreciation_schedule (id INT, year INT, gross_val NUMERIC, annual_depreciation_expense NUMERIC);"))
            conn.execute(text("TRUNCATE TABLE equipment_depreciation_schedule;"))

            depreciation_sql = """
            INSERT INTO equipment_depreciation_schedule (id, year, gross_val, annual_depreciation_expense)
            WITH ppe_base AS (
            SELECT 
                id,
                EXTRACT(YEAR FROM payment_date)::INT AS start_year,
                amount AS cost
            FROM payments 
            WHERE payment_type = 'equipment'
            )
            SELECT 
                p.id,
                c.year,
                p.cost AS gross_val,
                CASE 
                    WHEN c.year = p.start_year THEN (p.cost / 10.0) * 0.5
                    WHEN c.year = p.start_year + 10 THEN (p.cost / 10.0) * 0.5
                    WHEN c.year > p.start_year AND c.year < p.start_year + 10 THEN (p.cost / 10.0)
                    ELSE 0 
                END AS annual_depreciation_expense
            FROM ppe_base p
            CROSS JOIN (SELECT DISTINCT EXTRACT(YEAR FROM calendar_at)::INT AS year FROM calendar) c
            WHERE c.year BETWEEN p.start_year AND p.start_year + 10
            """
            conn.execute(text(depreciation_sql))

            # 2. Create and populate Accruals
            conn.execute(text("CREATE TABLE IF NOT EXISTS expense_accrual_schedule (id INT, account VARCHAR, amount NUMERIC, accrual_date DATE, cash_payment_date DATE);"))
            conn.execute(text("TRUNCATE TABLE expense_accrual_schedule;"))

            accrual_sql = """
            INSERT INTO expense_accrual_schedule (id, account, amount, accrual_date, cash_payment_date)
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
            """  
            conn.execute(text(accrual_sql))
            conn.commit()
            logger.info("Custom financial tables created successfully.")

        except Exception as e:
            logger.error(f"Error creating custom financial tables: {e}")

def create_focus_view(end_year=2023):  
    engine = get_engine() 
    with engine.connect() as conn:
        try:
            sql = f"""
            CREATE OR REPLACE VIEW dashboard_flux_analysis AS
            WITH pl_source AS (
            -- 1. REVENUE
                SELECT 
                    EXTRACT(YEAR FROM sale_at)::INT AS year, 
                    'Revenue' AS account, 
                    SUM(quantity * price) AS amt 
                FROM sales 
                GROUP BY 1
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

                SELECT EXTRACT(YEAR FROM payment_date)::INT AS year, 
                'Cash' AS account, 
                -SUM(amount) AS amt
                FROM payments 
                WHERE payment_type IN ('equipment', 'interest', 'wage', 'utility', 'tax', 'loan', 'rent')
                GROUP BY 1
                UNION ALL

                -- 5. COGS (P&L Impact)
                SELECT EXTRACT(YEAR FROM s.sale_at)::INT AS year, 
                'COGS' AS account, 
                -SUM(pc.amount * s.quantity) 
                FROM sales s 
                LEFT JOIN (
                    SELECT 
                        DISTINCT product_name, 
                        amount 
                        FROM purchases) pc 
                        ON s.product_name = pc.product_name 
                GROUP BY 1
                UNION ALL

                -- 6. DEPRECIATION EXPENSE (P&L Impact)
                SELECT 
                year, 
                'Depr_Exp' AS account, 
                -SUM(annual_depreciation_expense) AS amt
                FROM equipment_depreciation_schedule 
                GROUP BY 1
                UNION ALL

                -- 7. PPE SNAPSHOT (Balance Sheet Anchor - Flat 31k)
                SELECT 
                year, 
                'PPE_Snapshot' AS account, 
                SUM(gross_val) AS amt
                FROM equipment_depreciation_schedule 
                GROUP BY 1
                UNION ALL

                -- 8. ACCOUNTS RECEIVABLE (Year-end unpaid sales)
                SELECT EXTRACT(YEAR FROM payment_at)::INT AS year, 
                'Accounts_Receivable' AS account, 
                SUM(price * quantity) AS amt
                FROM sales WHERE payment_method <> 'cash' AND DATE_PART('month', payment_at) = 12 
                GROUP BY 1
                UNION ALL

                -- 9. INVENTORY: Additions (Purchases)
                SELECT EXTRACT(YEAR FROM purchase_at)::INT AS year, 'Inventory' AS account, SUM(amount * quantity) AS amt
                FROM purchases GROUP BY 1
                UNION ALL
                
                -- 10. INVENTORY: Deductions (Sales at Cost)
                SELECT EXTRACT(YEAR FROM sale_at)::INT AS year, 'Inventory' AS account, -SUM(s.quantity * pc.amount) AS amt
                FROM sales AS s
                LEFT JOIN (SELECT DISTINCT product_name, amount FROM purchases) AS pc ON s.product_name = pc.product_name
                GROUP BY 1

                UNION ALL
                -- 11. LOAN BALANCE (Liability)
                -- Initial Loan Value (Increases Liability)
                SELECT EXTRACT(YEAR FROM loan_at)::INT AS year, 'Loan_Principal' AS account, SUM(value) AS amt 
                FROM loans GROUP BY 1
                UNION ALL
                -- Principal Payments (Decreases Liability)
                SELECT EXTRACT(YEAR FROM payment_date)::INT AS year, 'Loan_Principal' AS account, -SUM(amount) AS amt 
                FROM payments WHERE payment_type = 'loan' GROUP BY 1

                UNION ALL
                -- 12. OPERATIONAL EXPENSES (P&L Accrual - the month incurred)
                SELECT 
                    EXTRACT(YEAR FROM accrual_date)::INT AS year, 
                    account, 
                    -SUM(amount) AS amt
                FROM expense_accrual_schedule GROUP BY 1, 2

                UNION ALL
                -- 13. ACCOUNTS PAYABLE: Increase (When Accrued)
                SELECT EXTRACT(YEAR FROM accrual_date)::INT AS year, 
                'Accounts_Payable' AS account, 
                SUM(amount) AS amt
                FROM expense_accrual_schedule 
                GROUP BY 1, 2
                UNION ALL

                -- 14. ACCOUNTS PAYABLE: Decrease (When Paid)
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
                ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Cash'), 0)::numeric, 2) AS cash,
                ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Accounts_Receivable'), 0)::numeric, 2) AS accounts_receivable,
                ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Accounts_Payable'), 0)::numeric, 2) AS accounts_payable,

                -- Long-Term Liabilities
                ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Loan_Principal'), 0)::numeric, 2) AS debt_remaining,
                
                -- CURRENT ASSETS
                ROUND(COALESCE(MAX(running_balance) FILTER (WHERE account = 'Inventory'), 0)::numeric, 2) AS inventory,

                -- FIXED ASSETS: Net PPE calculation
                ROUND(COALESCE(MAX(annual_movement) FILTER (WHERE account = 'PPE_Snapshot'), 0)::numeric, 2) AS gross_ppe,
                ROUND(GREATEST(0, 
                    COALESCE(MAX(annual_movement) FILTER (WHERE account = 'PPE_Snapshot'), 0) + 
                    COALESCE(MAX(running_balance) FILTER (WHERE account = 'Depr_Exp'), 0)
                )::numeric, 2) AS net_ppe
            FROM yearly_summaries
            WHERE year <= {end_year} 
            GROUP BY year
            ORDER BY year        
            """
            conn.execute(text(sql))
            conn.commit()
            logger.info(f"Analytical views refreshed in Docker DB for end_year {end_year}.")

        except Exception as e:
            logger.error(f"Error during transformations: {e}")      


