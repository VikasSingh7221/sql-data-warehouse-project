/* Here we create the DDL for the gold layer */
/* This layer contains the dimensional as well as facts model */

CREATE OR REPLACE VIEW gold.dim_customer AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- surrogate key
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.cntry AS country,
        ci.cst_marital_status AS martial_status,
        CASE 
            WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen,'N/A') 
        END AS gender,
        ca.bdate AS birth_date,
        ci.cst_create_date AS create_date 
    FROM 
        silver.crm_cust_info ci
    LEFT JOIN 
        silver.erp_cust_az12 ca 
    ON ci.cst_key = ca.cid
    LEFT JOIN 
        silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
    

CREATE OR REPLACE VIEW gold.dim_product AS
    SELECT 
        ROW_NUMBER() OVER(ORDER BY pin.prd_start_dt) as product_key, -- surrogate key
        pin.prd_id AS product_id,
        pin.prd_key AS product_number,
        pin.prd_nm AS product_name,
        pin.cat_id AS category_id,
        cg.cat AS category,
        cg.subcat AS subcategory,
        cg.maintenance AS maintenance,
        pin.prd_cost AS product_cost,
        pin.prd_start_dt AS product_start_date,
        pin.prd_end_dt AS product_end_data
    FROM 
        silver.crm_prd_info pin 
    LEFT JOIN 
        silver.erp_px_cat_g1v2 cg 
    ON pin.cat_id = cg.id
    WHERE pin.prd_end_dt IS NULL;


CREATE OR REPLACE VIEW gold.fact_sales AS
    SELECT
        sd.sls_ord_num  AS order_number,
        pr.product_key  AS product_key,
        cu.customer_key AS customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt  AS shipping_date,
        sd.sls_due_dt   AS due_date,
        sd.sls_sales    AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price    AS price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_product pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customer cu
        ON sd.sls_cust_id = cu.customer_id;