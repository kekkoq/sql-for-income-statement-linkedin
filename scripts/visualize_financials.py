import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

def create_financial_plots():
    # 1. Clear memory to fix the wide green bar issue
    plt.close('all') 
    
    # 2. PATH LOGIC: Move UP one level to create 'Images' in the root
    script_dir = os.path.dirname(os.path.abspath(__file__)) # current folder: /scripts
    project_root = os.path.dirname(script_dir)             # root folder: /workspace
    output_folder = os.path.join(project_root, 'Images')
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, 'p_and_l_chart_fixed.png')

    try:
        # 3. Connection & Query
        conn = psycopg2.connect(
            host="db", database="postgres", user="postgres", password="postgres", port="5432"
        )
        query = "SELECT * FROM dashboard_flux_analysis WHERE year IN (2021, 2022) ORDER BY year"
        df = pd.read_sql_query(sql=query, con=conn)
        conn.close()
        
        # 4. Data Setup
        years = df['year'].astype(str).tolist()
        x = np.arange(len(years))
        width = 0.15 # Skinny bars
        df['total_expenses'] = df['revenue'] - df['net_income']
            
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

        # --- Chart 1: P&L Overview ---
        # Plotting each only ONCE to fix the legend and width
        ax1.bar(x - width, df['revenue'], width, label='Total Revenue', color='#2ecc71')
        ax1.bar(x, df['total_expenses'], width, label='Total Expenses', color='#e74c3c') 
        ax1.bar(x + width, df['net_income'], width, label='Net Income', color='#3498db')

        ax1.set_title('P&L Overview: Revenue, Expenses, & Profit', fontsize=14, pad=15)
        ax1.set_ylabel('Amount ($)')
        ax1.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: f'${x*1e-6:.1f}M'))
        ax1.set_xticks(x)
        ax1.set_xticklabels(years)
        ax1.legend()
        ax1.grid(True, linestyle='--', alpha=0.6)

        # --- Chart 2: Balance Sheet Comparison ---
        metrics = ['cash', 'debt_remaining', 'inventory']
        x_bs = np.arange(len(metrics))
        ax2.bar(x_bs - width/2, df.iloc[0][metrics], width, label='2021', color='#9b59b6')
        ax2.bar(x_bs + width/2, df.iloc[1][metrics], width, label='2022', color='#e67e22')
        ax2.set_title('Balance Sheet Comparison (2021 vs 2022)', fontsize=14, pad=15)
        ax2.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: f'${x*1e-6:.1f}M'))
        ax2.set_xticks(x_bs)
        ax2.set_xticklabels(['Cash on Hand', 'Debt Remaining', 'Inventory Value'])
        ax2.legend()
        ax2.grid(axis='y', linestyle='--', alpha=0.7)

        # 5. Save and Print
        plt.tight_layout()
        plt.savefig(output_path, dpi=300) 
        print(f"✅ Success! File saved in ROOT Images folder: {output_path}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_financial_plots()