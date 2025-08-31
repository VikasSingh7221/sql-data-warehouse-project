
CREATE OR REPLACE PROCEDURE silver.silver_data_load(log_table STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE log_stmt STRING;
BEGIN

    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.crm_cust_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.crm_cust_info;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.crm_cust_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE UPPER(TRIM(cst_marital_status)) WHEN 'S' THEN 'Single' WHEN 'M' THEN 'Married' ELSE 'N/A' END AS cst_marital_status,
        CASE UPPER(TRIM(cst_gndr)) WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'N/A' END AS cst_gndr,
        cst_create_date
    FROM (
            SELECT 
                *, 
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
            FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
        ) 
    WHERE flag_last=1;



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.crm_prd_info'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.crm_prd_info;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.crm_prd_info'')';
    EXECUTE IMMEDIATE :log_stmt;

    INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
    SELECT 
    	prd_id,
    	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    	prd_nm,
    	NVL(prd_cost,0) AS prd_cost,
    	CASE 
    		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    		ELSE 'N/A'
    	END AS prd_line,
    	prd_start_dt,
    	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt 
    FROM bronze.crm_prd_info;



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.crm_sales_details'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.crm_sales_details;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.crm_sales_details'')';
    EXECUTE IMMEDIATE :log_stmt;

    INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
    SELECT 
    	sls_ord_num,
    	sls_prd_key,
    	sls_cust_id,
    	CASE 
    		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN NULL
    		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    	END AS sls_order_dt,
    	CASE 
    		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::VARCHAR) != 8 THEN NULL
    		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    	END AS sls_ship_dt,
    	CASE 
    		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::VARCHAR) != 8 THEN NULL
    		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    	END AS sls_due_dt,
    	CASE 
    		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
    			THEN sls_quantity * ABS(sls_price)
    		ELSE sls_sales
    	END AS sls_sales, 
    	sls_quantity,
    	CASE 
    		WHEN sls_price IS NULL OR sls_price <= 0 
    			THEN sls_sales / NULLIF(sls_quantity, 0)
    		ELSE sls_price  
    	END AS sls_price
    FROM bronze.crm_sales_details;



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.erp_cust_az12'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.erp_cust_az12;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.erp_cust_az12'')';
    EXECUTE IMMEDIATE :log_stmt;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid, 
        CASE
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate, 
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END AS gen 
    FROM bronze.erp_cust_az12;



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.erp_loc_a101'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.erp_loc_a101;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.erp_loc_a101'')';
    EXECUTE IMMEDIATE :log_stmt;

    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT 
        REPLACE(cid,'-','') AS cid,
        CASE 
            WHEN TRIM(UPPER(cntry)) IN ('US','USA') THEN 'United States'
            WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
    		ELSE TRIM(cntry)
        END AS cntry
    FROM 
        bronze.erp_loc_a101;



    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Truncating Table silver.erp_px_cat_g1v2'')';
    EXECUTE IMMEDIATE :log_stmt;
    
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    
    log_stmt := 'INSERT INTO ' || log_table || '(message)
                 VALUES (''Loading data in Table silver.erp_px_cat_g1v2'')';
    EXECUTE IMMEDIATE :log_stmt;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT
        id,
        cat,
        subcat,
        maintenance
	FROM bronze.erp_px_cat_g1v2;

RETURN 'Silver data load completed successfully';
END;
$$;

-- To execute the procedure, provide the log table name.
call silver. silver_data_load('log_table');