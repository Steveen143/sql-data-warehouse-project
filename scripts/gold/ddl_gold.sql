/*
=====================================================================
DDL Script: Create Gold Views
=====================================================================
Script Purpose: 
    Tis script creates views for the Gold Layer in the data warehouse.
    The Gold Layer represent the final dimension and fact tables (Star Schema).

    Each views performs transformations and combines data from the silver layer to produce a clean, enriched,
    and business ready dataset.

Usage:
    -This views can be queried directly for analytics and reporting.
=====================================================================

*/


==============================================
  --Create Dimension: gold.dim_customers
==============================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL

DROP gold.dim_customer;

GO

CREATE VIEW gold.dim_customers AS

	SELECT 
			ROW_NUMBER () OVER(ORDER BY cst_id) AS custormer_key,
			ci.cst_id AS customer_id,
			ci.cst_key AS customer_number,
			ci.cst_firstname AS first_name,
			ci.cst_lastname AS last_name,
			ci.cst_marital_status AS marital_status,
			CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the master for gender infor (preference wise)
				ELSE COALESCE (ca.gen, 'Unknown')
			END AS gender,
			la.cntry AS country,
			ca.bdate AS birthdate,
			ci.cst_create_date AS create_date
		FROM silver.crm_cust_info AS ci
		LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.cid
		LEFT JOIN silver.erp_loc_a101 AS la
		ON ci.cst_key = la.cid;

GO

  ==============================================
  -- Create Dimension: gold.dim_products
  ==============================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL

DROP gold.dim_products;

  --Creation of Dimenssion products

	CREATE VIEW gold.dim_products AS

	SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id, pn.prd_key AS product_number, 
	pn.prd_nm AS product_name, 
	pn.cat_id AS category_id,
	pc.cat AS category, 
	pc.subcat AS subcategory, pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line, pn.prd_start_dt  AS start_date

	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL; --To filter out all historical data

  ==============================================
  -- Create Dimension: gold.fact_sales
  ==============================================


  
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL

DROP gold.fact_sales;

GO
  
CREATE VIEW gold.fact_sales AS

	SELECT sls_ord_num AS order_number,
		pr.product_key, 
		cu.custormer_key,--This supposed friendly name from the gold dim customer layer has an 'r'  in between the 'om' of the customer (mistake)
		sls_order_dt AS order_date,
		sls_ship_dt AS shipping_date, sls_due_dt AS due_date,
		sls_sales AS sales_amount, sls_quantity AS quantity, sls_price AS price

	FROM silver.crm_sales_details sd
		LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
		LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id
