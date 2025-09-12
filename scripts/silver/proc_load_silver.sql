/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @total_start_time DATETIME;
    BEGIN TRY

        PRINT '====================================='
        PRINT 'Loading Silver Layer'
        PRINT '====================================='

        PRINT '-------------------------------------'
        PRINT 'Loading CRM DATA'
        PRINT '-------------------------------------'


        SET @start_time = GETDATE();
		SET @total_start_time = GETDATE();
        PRINT '>> Truncating silver.crm_cust_info table'

        TRUNCATE TABLE  silver.crm_cust_info;

        PRINT '>> Inserting data into silver.crm_cust_info table'

        Insert into silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )

        Select
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        from (
        Select
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
        from bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        ) t where flag_last = 1;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.crm_prd_info table'

        TRUNCATE TABLE  silver.crm_prd_info;

        PRINT '>> Inserting data into silver.crm_prd_info table'



        Insert into silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )

        Select
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) as prd_cost,
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END as prd_line,
        prd_start_dt,
        DATEADD(day, -1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key order by prd_start_dt)) as prd_end_dt
        from bronze.crm_prd_info;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.crm_sales_details table'

        TRUNCATE TABLE  silver.crm_sales_details;

        PRINT '>> Inserting data into silver.crm_sales_details table'


        Insert into silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )

        Select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt as varchar) as date)
        END as sls_order_dt,
        CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt as varchar) as date)
        END as sls_ship_dt,
        CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt as varchar) as date)
        END as sls_due_dt,
        CASE WHEN sls_sales is NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END as sls_sales,
        sls_quantity,
        CASE WHEN sls_price is NULL or sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END as sls_price
        from bronze.crm_sales_details;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';


        PRINT '-------------------------------------'
        PRINT 'Loaded CRM DATA'
        PRINT '-------------------------------------'


        PRINT '-------------------------------------'
        PRINT 'Loading ERP DATA'
        PRINT '-------------------------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_cust_az12 table'

        TRUNCATE TABLE  silver.erp_cust_az12;

        PRINT '>> Inserting data into silver.erp_cust_az12 table'


        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )

        Select
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid)) -- Remove NAS from prefix if present
            ELSE cid
        END as cid,
        CASE WHEN bdate > GETDATE() THEN NULL 
            ELSE bdate
        END as bdate, -- Set future birthdays to null
        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END as gen -- Normalize gender values and handle unknown cases
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_loc_a101 table'

        TRUNCATE TABLE  silver.erp_loc_a101;

        PRINT '>> Inserting data into silver.erp_loc_a101 table'


        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )

        SELECT 
        REPLACE(cid, '-', '') AS cid, 
        CASE WHEN UPPER(TRIM(cntry)) in ('US', 'USA') THEN 'United States'  
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE cntry
        END as cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating silver.erp_px_cat_g1v2 table'

        TRUNCATE TABLE  silver.erp_px_cat_g1v2;

        PRINT '>> Inserting data into silver.erp_px_cat_g1v2 table'


        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

        PRINT '-------------------------------------'
        PRINT 'Loaded ERP DATA'
        PRINT '-------------------------------------'

        PRINT '===============================================';
		PRINT 'Loaded Silver Layer';
		PRINT '-----------------------------------------------';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @total_start_time, @end_time) as varchar) + 's';
		PRINT '===============================================';
    END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'Error occurred while loading Silver Layer';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST(ERROR_NUMBER() as varchar);
		PRINT 'Error message' + CAST(ERROR_STATE() as varchar);
		PRINT '===============================================';
	END CATCH
END