# 🚀 Snowflake ELT Pipeline (Bronze → Silver → Gold)

This project demonstrates how to build a **layered data warehouse** in **Snowflake** with:
- Bronze, Silver, and Gold schemas
- Data ingestion from staged CSV files
- Stored procedure for automated loads
- Logging for auditing

---

## 📂 Project Structure
- **datasets/** → CSV input files (CRM + ERP)
- **bronze/** → Raw ingestion tables
- **silver/** → Cleaned & transformed tables
- **gold/** → Business-ready marts
- **procedures/** → Snowflake Stored Procedures
- **log_table** → Central log for load status

---

## ⚙️ Setup Steps

### 1. Create Database & Schemas
- Make a new Snowflake database (e.g., `DataWarehouse`).
- Create three schemas: `bronze`, `silver`, `gold`.

### 2. Create Tables
- Bronze layer contains raw tables for:
  - **CRM** (customers, products, sales)
  - **ERP** (customer master, locations, product categories)

### 3. Create Logging Table
- `log_table` will capture messages (e.g., truncate/load status) with timestamps.

### 4. Create File Format & Stage
- Define a `CSV` file format.
- Create a Snowflake **Stage** (e.g., `bronze.csv_stage`).
- Upload all dataset CSV files into this stage.

### 5. Data Load Procedure
- A **Stored Procedure** `bronze_data_load(log_table)`:
  - Truncates bronze tables
  - Copies fresh data from stage
  - Inserts log messages into `log_table`

### 6. Execute the Pipeline
- Call procedure:
  ```sql
  CALL bronze_data_load('log_table');
