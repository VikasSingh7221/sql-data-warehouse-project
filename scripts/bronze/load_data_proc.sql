/* Here we are creating :
    1. Log Table to store logs.
    2. File_format for stage.
    3. Snowflake_stage(internal named stage) to put csv_data.
    4. SP to Load data from snowflake_stage.
*/

-- create log_table to store logs..
CREATE TABLE log_table(
    message     STRING,
    executed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- create file format.
CREATE OR REPLACE FILE FORMAT bronze.csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ',';

 -- create stage.
 CREATE OR REPLACE STAGE bronze.csv_stage FILE_FORMAT = bronze.csv_format;
 
 -- sp to load data.
CREATE OR REPLACE PROCEDURE bronze_data_load(log_table STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE log_stmt STRING;
BEGIN

    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.crm_cust_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.crm_cust_info;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.crm_cust_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    COPY INTO bronze.crm_cust_info
    FROM @bronze.csv_stage/datasets/crm/cust_info.csv
    ON_ERROR = 'ABORT_STATEMENT';



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.crm_prd_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.crm_prd_info;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.crm_prd_info'')';
    EXECUTE IMMEDIATE :log_stmt;

    COPY INTO bronze.crm_prd_info
    FROM @bronze.csv_stage/datasets/crm/prd_info.csv
    ON_ERROR = 'ABORT_STATEMENT';



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.crm_sales_details'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.crm_sales_details;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.crm_sales_details'')';
    EXECUTE IMMEDIATE :log_stmt;

    COPY INTO bronze.crm_sales_details
    FROM @bronze.csv_stage/datasets/crm/sales_details.csv
    ON_ERROR = 'ABORT_STATEMENT';



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.erp_cust_az12'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.erp_cust_az12'')';
    EXECUTE IMMEDIATE :log_stmt;

    COPY INTO bronze.erp_cust_az12
    FROM @bronze.csv_stage/datasets/erp/CUST_AZ12.csv
    ON_ERROR = 'ABORT_STATEMENT';



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.erp_loc_a101'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.erp_loc_a101'')';
    EXECUTE IMMEDIATE :log_stmt;

    COPY INTO bronze.erp_loc_a101
    FROM @bronze.csv_stage/datasets/erp/LOC_A101.csv
    ON_ERROR = 'ABORT_STATEMENT';



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table bronze.erp_px_cat_g1v2'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table bronze.erp_px_cat_g1v2'')';
    EXECUTE IMMEDIATE :log_stmt;

    COPY INTO bronze.erp_px_cat_g1v2
    FROM @bronze.csv_stage/datasets/erp/PX_CAT_G1V2.csv
    ON_ERROR = 'ABORT_STATEMENT';

RETURN 'Bronze data load completed successfully';
END;
$$;

-- calling sp with parameter log_table.
call bronze_data_load('log_table');