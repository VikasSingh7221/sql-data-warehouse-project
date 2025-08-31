/* DDL Scripts For silver layer */

 CREATE OR REPLACE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(30),
	cst_lastname VARCHAR(30),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date	DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );


  CREATE OR REPLACE TABLE silver.crm_prd_info(
  	prd_id INT,
    cat_id VARCHAR(50),
	prd_key VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(20),
	prd_start_dt DATE,
	prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );


 CREATE OR REPLACE TABLE silver.crm_sales_details(
  	sls_ord_num VARCHAR(10),
	sls_prd_key VARCHAR(30),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );





 -- DDL Scripts For source_erp.

 CREATE  OR REPLACE TABLE silver.erp_cust_az12(
	cid VARCHAR(20),
	bdate DATE,
	gen VARCHAR(10),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );


CREATE  OR REPLACE TABLE silver.erp_loc_a101(
	cid VARCHAR(20),
	cntry VARCHAR(20),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );


 CREATE  OR REPLACE TABLE silver.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(20),
	subcat VARCHAR(20),
	maintenance VARCHAR(10),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 );


 -- DDL Scripts For Logging Table.
 CREATE TABLE silver.log_table(
    message     STRING,
    executed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);