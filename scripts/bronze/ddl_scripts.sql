/* Here we are writing DDL Scripts to create tables for source crm and erp.*/


--- DDL Scripts For source_crm.
 CREATE TABLE IF NOT EXISTS bronze.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(30),
	cst_lastname VARCHAR(30),
	cst_marital_status VARCHAR(2),
	cst_gndr VARCHAR(2),
	cst_create_date	DATE
 );


  CREATE OR REPLACE TABLE bronze.crm_prd_info(
  	prd_id INT,
	prd_key VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(2),
	prd_start_dt DATE,
	prd_end_dt DATE
 );


 CREATE TABLE IF NOT EXISTS bronze.crm_sales_details(
  	sls_ord_num VARCHAR(10),
	sls_prd_key VARCHAR(30),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
 );





 -- DDL Scripts For source_erp.

 CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12(
	cid VARCHAR(20),
	bdate DATE,
	gen VARCHAR(10)
 );


CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101(
	cid VARCHAR(20),
	cntry VARCHAR(20)
 );


 CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(20),
	subcat VARCHAR(20),
	maintenance VARCHAR(10)
 );