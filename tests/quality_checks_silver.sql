-- ========CRM Customer Info Table QCs===========

-- Check for Nulls or duplicates in Primary Key
-- Expectations: No Result

Select
cst_id,
COUNT(*) 
from silver.crm_cust_info
group by cst_id
having COUNT(*) > 1 or cst_id is NULL;

-- Check for unwanted spaces
-- Expectation: No Results
Select
cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

Select
cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
Select
distinct cst_gndr
from silver.crm_cust_info;

Select
distinct cst_marital_status
from silver.crm_cust_info;


-- =============CRM Product Info Table QCs=============

-- Check for Nulls or duplicates in Primary Key
-- Expectations: No Result

Select 
prd_id,
COUNT(*)
from silver.crm_prd_info
GROUP BY prd_id
having COUNT(*) > 1 or prd_id is NULL;

-- Check for unwanted spaces
-- Expectations: No Result
Select
prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Check for null or negative numbers
-- Expectations: No result
Select prd_cost
from silver.crm_prd_info
where prd_cost is NULL or prd_cost < 0;

-- Data Standardization & Consistency
Select distinct prd_line
from silver.crm_prd_info;

-- Check for invalida date orders
Select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt


-- ============CRM Sales Details Table QCs================

-- Check for Nulls in Order Num
-- Expectations: No Result
Select *
from silver.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num);

-- Check for Invalid dates
Select
NULLIF(sls_order_dt, 0) as  sls_order_dt
from silver.crm_sales_details 
where sls_order_dt <= 0 
or LEN(sls_order_dt) != 8 
or sls_order_dt > 20500101 
or sls_order_dt < 19000101;

Select
NULLIF(sls_ship_dt, 0) as  sls_ship_dt
from silver.crm_sales_details 
where sls_ship_dt <= 0 
or LEN(sls_ship_dt) != 8 
or sls_ship_dt > 20500101 
or sls_ship_dt < 19000101;

Select
NULLIF(sls_due_dt, 0) as  sls_due_dt
from silver.crm_sales_details 
where sls_due_dt <= 0 
or LEN(sls_due_dt) != 8 
or sls_due_dt > 20500101 
or sls_due_dt < 19000101;

-- Check for Invalid date orders
Select * from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check for data consistency between Sales, Quanitity and Price
-- >> Sales  = Quantity * Price
-- >> Values must not be null, zero or negative.
-- Expectations: No Result
Select
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details 
where sls_sales != sls_quantity* sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;


-- ================ERP Customer Data table QCs===============

-- Identify out of range dates
-- Expectations: No Result
SELECT
bdate
FROM silver.erp_cust_az12
where bdate < '1925-01-01' or bdate > GETDATE();


-- Data Standardization & Consistency
Select distinct gen from silver.erp_cust_az12;


-- =============ERP Customer Location Table QCs===============

-- Data Standardization & Consistency
Select 
distinct cntry 
from silver.erp_loc_a101
order by cntry;



-- =============ERP Product Category Table QCs================

-- Check for unwanted spaces
-- Expectations: No Result
Select * from silver.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) 
or maintenance != TRIM(maintenance)


-- Data Standardization & Consistency
Select 
distinct cat
from silver.erp_px_cat_g1v2

Select 
distinct subcat
from silver.erp_px_cat_g1v2

Select 
distinct maintenance
from silver.erp_px_cat_g1v2