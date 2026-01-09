from utils import get_engine, logger
from sqlalchemy import text 

def audit_2021_cashflow():
    """Audits the cash for the year 2021."""
    engine = get_engine()

    results = {
    "sales_cash_in": 0.0,
    "loan_in": 0.0,
    "purchase_out": 0.0,   
    "expense_out": 0.0,
            }

    try:
        with engine.connect() as conn: # Connection opens here
            logger.info("Connection established. Querying 2021 data...")
            
            # 1. Get the credit sales
            results["sales_cash_in"] = conn.execute(text("""  
                SELECT 
                    SUM(quantity * price) 
                FROM sales
                WHERE EXTRACT(
                    YEAR FROM (CASE WHEN payment_method = 'cash' THEN sale_at ELSE sale_at + INTERVAL '1 month' END)) = 2021
            """)
            ).scalar() or 0.0
            
            # 2. Get the loan inflow
            results["loan_in"] = conn.execute(text("""  
                SELECT 
                    SUM(value) 
                FROM loans
                WHERE EXTRACT(YEAR FROM loan_at) = 2021
            """)
            ).scalar() or 0.0

            # 3. Get the purchase outflow
            results["purchase_out"] = conn.execute(text("""
                SELECT 
                    SUM(quantity * amount)
                FROM purchases 
                WHERE EXTRACT(
                    YEAR FROM (CASE WHEN payment_method = 'cash' THEN purchase_at ELSE purchase_at + INTERVAL '1 month' END)) = 2021
            """)
            ).scalar() or 0.0

            # 4. Get the expense outflow
            results["expense_out"] = conn.execute(text("""
                SELECT 
                    SUM(amount)
                    FROM payments
                    WHERE EXTRACT(YEAR FROM payment_date) = 2021
            """)
            ).scalar() or 0.0

            # 5. Get the Dashboard View's reported cash
            view_cash = conn.execute(text("""
                SELECT 
                    cash
                FROM dashboard_flux_analysis 
                WHERE year = 2021
            """)
            ).scalar() or 0.0

            net_cash_calculated = results["sales_cash_in"] + results["loan_in"] - results["purchase_out"] - results["expense_out"]

            # 3. Log the results
            logger.info("2021 Cash Audit:")
            for key, value in results.items():
                logger.info(f"{key.replace('_', ' ').title()}: ${value:,.2f}")

            logger.info(f"calculated Ending Cash (2021): ${net_cash_calculated:,.2f}")
            logger.info(f"View Reported Cash (2021): ${view_cash:,.2f}")

            if abs(net_cash_calculated - view_cash) < 0.01:
                logger.info("✅ SUCCESS: Cash reconciliation matches!")
            else:
                diff = net_cash_calculated - view_cash
                logger.warning(f"⚠️ Discrepancy of {diff:,.2f} found.")

    except Exception as e:
        logger.error(f"An error occurred during the cash audit: {e}")
            
if __name__ == "__main__":
    audit_2021_cashflow()
