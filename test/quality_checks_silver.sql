/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose: 
    This script performs various quality checks for data consistency, accuracy, and 
    standardization across the 'silver' schemas. It includes checks for:
        -Null or duplicate primary keys.
        - Unwanted spaces in string fields.
        - Data standardization and consistencies.
        -Invalid date ranges and orders.
        - Data consistency between related fields.


Usage Notes:
  -Run these checks after data loading Silver Layer.
  - Investigate and resolve any disvcrepancies found during the checks.

*/





-- First check for NULLS or Duplicates in the primary key
  -- Expectation: No Result
  SELECT ci.cst_id, COUNT(*)
  FROM bronze.crm_cust_info ci
  GROUP BY ci.cst_id
  HAVING COUNT(*) > 1 OR cst_id IS NULL;

  SELECT *
  FROM (

    SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info )t
  WHERE flag_last = 1;


  --Check for Unwanted spaces\Unwanted spaces elimination
  --Expectation: No Results
  SELECT 
  TRIM (cst_firstname)
  FROM bronze.crm_cust_info
  WHERE cst_firstname != TRIM (cst_firstname)--Check for Unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--Check for the Data Standardization & Consistency
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
--Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

  --Fixing Date Inconsistency
  SELECT 
  prd_id,
  prd_key,
  prd_nm,
  prd_start_dt,
  prd_end_dt,
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
  FROM bronze.crm_prd_info 
  WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

  --Check for invalid Date Orders
  SELECT *
  FROM bronze.crm_prd_info 
  WHERE prd_end_dt < prd_start_dt;

--Check Data consistency between: Between sales, quantity, and proce
--> sales = quantity * price
--> values must not be NULL, negetive or zero


SELECT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)

ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price <= 0 OR sls_sales IS NULL
THEN sls_sales / NULLIF(sls_quantity, 0)

ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <0
ORDER BY sls_sales, sls_quantity, sls_price;


--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check for Invalid Dates
SELECT 
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt >  20500101
OR sls_due_dt < 19000101

