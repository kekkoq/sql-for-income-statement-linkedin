# SQL for Finance: Income Statement Project (ELT Python + BI Workflow)

This is the repository for the LinkedIn Learning course SQL for Finance: Income Statement Project. 

**Disclaimer:**
This project is based on the dataset and exercises from the LinkedIn Learning course SQL for Finance: Income Statement Project.
The data included in this repository is provided solely for educational and portfolio demonstration purposes.

This repository extends the original course material by implementing a containerized ELT workflow, a PostgreSQL development environment, and a VS Code devcontainer setup suitable for analytics engineering, BI development, and reproducible SQL workflows.
It also documents the real‑world troubleshooting required to stabilize PostgreSQL inside a devcontainer environment after breaking changes were introduced in newer PostgreSQL Docker images.

## Project Overview
This project demonstrates:
- Loading raw financial data (loans, payments, purchases, sales) into PostgreSQL
- Transforming the data using SQL
- Preparing an income‑statement‑ready dataset
- Building a reproducible analytics environment using Docker + VS Code Devcontainers
- Documenting the ELT workflow for BI tools (Power BI)
The goal is to simulate a realistic analytics engineering workflow that a financial analyst or BI developer would use in a modern data stack.

## Development Environment
This project uses:
  - Docker Compose to orchestrate:
  - a PostgreSQL database (db)
- an application/devcontainer service (app)
- VS Code Devcontainers for a reproducible development environment
- PostgreSQL 16 (pinned version for stability)
- Startup SQL scripts to automatically create tables and load data

## PostgreSQL 18 Breaking Changes
The original project used:
Yaml
`image: postgres:latest`

However, the postgres:latest tag began pointing to PostgreSQL 18, which introduced breaking changes to the internal data directory structure.
This caused:
- PostgreSQL to fail during initialization
- The db container to enter a restart loop
- The hostname db to never resolve
- The devcontainer postCreateCommand to fail
- The startup SQL script to error with:
`could not translate host name "db" to address `

To fix this, the project now pins PostgreSQL to a stable version:
Yaml
`image: postgres:16 `
This ensures compatibility with the devcontainer environment and the startup scripts

## Fixes Applied (Troubleshooting Log)
During setup, several issues were encountered due to:
- incompatible PostgreSQL versions
- corrupted Docker volumes
- hidden devcontainer containers
- VS Code reusing old containers silently
  
Below is the documented resolution process.
1. Remove all containers (including hidden devcontainer containers). 
  Powershell
  `docker rm -f $(docker ps -aq) `
  This ensures no container is still attached to old volumes.

2. Remove all project-related volumes.
  Powershell
  `docker volume rm -f sql-for-income-statement-linkedin_postgres-data`
  `docker volume rm -f sql-for-income-statement-linkedin_devcontainer_postgres-data `
  These volumes contained incompatible PostgreSQL data directories.

3. Pin PostgreSQL to a stable version.  
  In `docker-compose.yml`:
  Yaml
  `image: postgres:16`

4. Rebuild the devcontainer.   
  In VS Code:
  `Ctrl + Shift + P → Dev Containers: Rebuild and Reopen in Container`

5. Verify the database is running
  Inside the devcontainer terminal:
  Bash
  `PGPASSWORD=postgres psql -h db -U postgres -d postgres -c "\dt"`

## Data Dictionary

### 1. Table: `sales`
This table represents the primary revenue stream for the business.

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `sale_at` | TIMESTAMP | The date and time the sale occurred. Primary key for time-series revenue analysis. |
| `quantity` | INTEGER | The number of units sold in the transaction. |
| `price` | NUMERIC | The unit price at the time of the transaction. |
| `product_id` | INTEGER | Foreign key referencing the product catalog. |

### 2. Table: `purchases`
This table tracks inventory acquisition and cash outflows.

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `is_account` | BOOLEAN | Flag indicating if the purchase was made on credit (True) or cash (False). |
| `delivery_day` | DATE | The scheduled date for the arrival of goods. |
| `payment_at` | TIMESTAMP | The actual timestamp when the cash left the account. |
| `delivery_at` | TIMESTAMP | The actual timestamp when the inventory was received. |

## Data Pipeline Workflow

1. Data Ingestion (Extract & Load)
   
Source: A 11MB PostgreSQL dump containing over 43,000 records of raw business activity.
Before loading new data, the pipeline performs a **Cascade Cleanup**.

Challenges & Soluctions:
- PostgreSQL prevents the deletion of tables if they are referenced by active Views.
  The script uses `DROP TABLE IF EXISTS ... CASCADE`. This ensures that any old analytical views are cleared before the schema is rebuilt, preventing "Object already exists" errors during the 11MB load.
- Loading the 11MB `setup-postgresql.sql` file (containing ~43,000+ commands).
  Instead of running the entire file as one giant string—which could crash the container's memory—the script reads the file and splits it into individual commands using the `;` delimiter.

Loop Execution: The pipeline iterates through each command one-by-one.

It uses a `try-except` block to skip "Empty Command" errors (common at the end of large SQL files) without stopping the entire migration.

Note: Once this data is loaded, the `extract_load()` function can be commented out in `main.py` to allow for faster subsequent runs of the analytical layers.`

1. Intermediate Financial Tables
   
Implemented accrual-to-cash timing adjustments, such as a 1-month lag for credit-based sales and purchases to model actual cash movement. To support accrual accounting and depreciation, the pipeline creates two tables before building the final report:

**equipment_depreciation_schedule**: Pre-calculates 10-year asset depreciation using a CROSS JOIN with a generated Calendar table.

**expense_accrual_schedule**: Shifts cash payments (wages, taxes, utilities) to the month they were actually incurred (Month-1 logic).

3. Analytical Layer: The Flux View
   
The final output is the dashboard_flux_analysis View.

Logic: This is a View that joins the raw sales data with the custom financial schedules.
Because it is a View, any small manual adjustment to the underlying sales or payments tables will be reflected in the Net Income and Gross Margin calculations instantly without re-running the Python script.

1. Data Validation & Auditing 
To ensure the reliability of the system, custom audit scripts in the /scripts folder perform two critical checks:

Revenue Audit (audit_revenue.py): Reconciles the aggregated financial views against granular sales records to confirm no data loss during transformation.

Cash Flow Reconciliation (audit_cash.py): Validates the "Ending Cash" position by modeling inflows (Sales, Loans) and outflows (Purchases, Operating Expenses) against the simulated cash timing rules

## How to Adjust the Pipeline
To modify the financial logic:

- To change raw data: Un-comment `extract_load()` in `main.py` and provide a new `.sql` source.
- To change math/ratios: Edit the SQL strings inside `create_focus_view()` in `pipeline.py`. Since it uses`CREATE OR REPLACE VIEW`, no need to re-run the script as many times as needed to tweak the formulas.

## Analytical Challenges & Solutions

**Automated Data Reconciliation**: To ensure the integrity of the 2021 Income Statement, a custom audit script was developed to perform a 1:1 reconciliation between the Aggregate Reporting View (dashboard_flux_analysis) and the Granular Source Transactions (sales). This process confirmed zero data loss during the ETL transformation.

**Infrastructure Connectivity**: Configured database communication within a Dockerized environment (hostname = db, not localhost) to ensure the Python transformation layer could reliably reach the PostgreSQL analytical warehouse.

**Connection Lifecycle Management**: Implemented best practices for database resource management using SQLAlchemy context managers. By ensuring all data fetching occurred within active transaction blocks, runtime crashes was prevented and data was consistent across multi-step audits.

**Schema Discovery & Documentation**: Conducted a manual audit of the source database schema to identify critical columns for financial reporting, such as sale_at for temporal filtering and is_account for purchase categorization












