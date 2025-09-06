/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

use DataWarehouse;

Drop TABLE if EXISTS silver.crm_cust_info;
Create table silver.crm_cust_info (
	cst_id int,
    cst_key Varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date,
    dwn_create_date datetime2 default GETDATE()
);

Drop TABLE if EXISTS silver.crm_prd_info;
Create table silver.crm_prd_info (
	prd_id int,
    cat_id varchar(50),
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwn_create_date datetime2 default GETDATE()
);

Drop TABLE if EXISTS silver.crm_sales_details;
Create table silver.crm_sales_details (
	sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwn_create_date datetime2 default GETDATE()
);

drop table if EXISTS silver.erp_cust_az12;
Create table silver.erp_cust_az12 (
	cid varchar(50),
    bdate date,
    gen varchar(50),
    dwn_create_date datetime2 default GETDATE()
);

drop table if EXISTS silver.erp_loc_a101;
Create table silver.erp_loc_a101 (
	cid varchar(50),
    cntry varchar(50),
    dwn_create_date datetime2 default GETDATE()
);

drop table if EXISTS silver.erp_px_cat_g1v2;
Create table silver.erp_px_cat_g1v2 (
	id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintenance varchar(50),
    dwn_create_date datetime2 default GETDATE()
);