import logging
import os
from sqlalchemy import create_engine

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Saves logs to a file
        logging.StreamHandler()             # Also prints to console
    ]
)

logger = logging.getLogger(__name__)

# --- DATABASE CONNECTION ---
def get_engine():
        user = "postgres"
        password = "postgres"  
        host = "db"     
        port = "5432"
        db = "postgres"

        try:
            engine = create_engine(
                f"postgresql://{user}:{password}@{host}:{port}/{db}"
            )
            logger.info("Successfully connected to the PostgreSQL database.")
            return engine
        except Exception as e:  
            logger.error(f"Error connecting to the PostgreSQL database: {e}")
            raise