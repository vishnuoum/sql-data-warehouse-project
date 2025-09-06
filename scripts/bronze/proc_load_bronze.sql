/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @total_start_time DATETIME;
	begin try
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		-- CRM Load
		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		SET @total_start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		PRINT '>> Inserting data into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		PRINT '>> Inserting data into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		-- ERP Load
		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'F:\devdir\data-warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as varchar) + 's';
		PRINT '-------------------';

		PRINT '===============================================';
		PRINT 'Loaded Bronze Layer';
		PRINT '-----------------------------------------------';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @total_start_time, @end_time) as varchar) + 's';
		PRINT '===============================================';
	end try
	begin catch
		PRINT '===============================================';
		PRINT 'Error occurred while loading Bronze Layer';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST(ERROR_NUMBER() as varchar);
		PRINT 'Error message' + CAST(ERROR_STATE() as varchar);
		PRINT '===============================================';
	end catch
end;