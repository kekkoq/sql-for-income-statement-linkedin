from utils import get_engine, logger
from sqlalchemy import text 

def audit_2021_revenue():
    """Audits the revenue for the year 2021."""
    engine = get_engine()

    # Initialize variables to 0.0 in case the queries fail
    view_revenue = 0.0
    raw_revenue = 0.0

    try:
        with engine.connect() as conn: # Connection opens here
            logger.info("Connection established. Querying 2021 data...")
            
            # 1. Get the number from your View
            view_query = text("SELECT revenue FROM dashboard_flux_analysis WHERE year = 2021")
            view_revenue = conn.execute(view_query).scalar() or 0.0
    
            # 2. Get the number from Raw Data (MOVED INSIDE THE WITH BLOCK)
            raw_query = text("""
                SELECT 
                    SUM(quantity * price) 
                FROM sales 
                WHERE EXTRACT(YEAR FROM sale_at) = 2021
            """)
            # Also fixed: use raw_query directly as it is already wrapped in text()
            raw_revenue = conn.execute(raw_query).scalar() or 0.0

            # 3. Compare and Report (MOVED INSIDE THE WITH BLOCK)
            logger.info("2021 Revenue Audit:")
            logger.info(f"View Revenue (2021): {view_revenue:,.2f}")
            logger.info(f"Raw Revenue (2021): {raw_revenue:,.2f}")

            if abs(view_revenue - raw_revenue) < 0.01:
                logger.info("The revenue figures match!")
            else:
                diff = view_revenue - raw_revenue
                logger.warning(f"Discrepancy of {diff:,.2f} found between the view and raw data.")

        # Connection automatically closes here once we exit the 'with' block

    except Exception as e:
        logger.error(f"An error occurred during the revenue audit: {e}")
            
if __name__ == "__main__":
    audit_2021_revenue()