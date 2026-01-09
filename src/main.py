from pipeline import extract_load, create_custom_financial_tables, create_focus_view
from utils import logger



def run_pipeline():
    # 1. Build the raw tables from your SQL script (Sales, Purchases, Payments)
    extract_load()
    logger.info("--- Raw Tables Created Successfully ---")

    # 2. Create the schedules needed for Net Income (Depreciation & Accruals)
    create_custom_financial_tables()
    logger.info("--- Custom Financial Tables Created ---")

    # 3. Build the final view for 2021-2022 Flux Analysis
    create_focus_view()
    logger.info("--- Flux Analysis View Created Successfully ---")

if __name__ == "__main__":
    run_pipeline()