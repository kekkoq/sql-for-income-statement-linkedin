from sqlalchemy import create_engine, text
import pandas as pd

# 1. Create an in-memory SQLite database
engine = create_engine('sqlite:///:memory:')

def test_accrual_logic():
    print("--- Starting Test ---") # This confirms the script is running
    with engine.connect() as conn:
        # Create dummy tables
        conn.execute(text("CREATE TABLE sales (sale_at DATE, quantity INT, price FLOAT)"))
        conn.execute(text("INSERT INTO sales VALUES ('2021-12-15', 10, 100)"))
        
        end_year = 2023
        # Using the f-string we discussed to inject the year
        sql = f"SELECT * FROM sales WHERE strftime('%Y', sale_at) <= '{end_year}'"
        
        result = pd.read_sql(sql, conn)
        
        if not result.empty:
            print("Success! Data found:")
            print(result)
            # 2. This creates the CSV file in your folder
            result.to_csv("test_output.csv", index=False)
            print("--- File 'test_output.csv' created successfully ---")
        else:
            print("Test ran, but the result was empty.")

if __name__ == "__main__":
    test_accrual_logic()