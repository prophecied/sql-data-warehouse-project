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

Create OR Alter Procedure bronze.load_bronze As 
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin Try
		Set @batch_start_time = Getdate();
		Print '======================================================';
		Print 'Loading Bronze Layer';
		Print '======================================================';

		Print '------------------------------------------------------';
		Print 'Loading CRM Table';
		Print '------------------------------------------------------';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info 

		Print '>>Inserting Data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info

		Print '>>Inserting Data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details

		Print '>>Inserting Data Into: bronze.crm_sales_details';
		Bulk Insert bronze.crm_sales_details
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';

		Print '------------------------------------------------------';
		Print 'Loading ERP Table';
		Print '------------------------------------------------------';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12

		Print '>>Inserting Data Into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101

		Print '>>Inserting Data Into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';

		Set @start_time = GETDATE();
		Print '>>Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2

		Set @start_time = GETDATE();
		Print '>>Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'D:\Project\Data warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		With (
			Firstrow = 2,
			Fieldterminator = ',',
			Tablock
		);Set @end_time = GETDATE();
		Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
		Print'>>---------------';

		Set @batch_end_time = Getdate();
		Print '================================================='
		Print 'Loading Bronze Layer is Completed';
	Print' - Total Load Duration: ' + Cast(Datediff(second, @batch_start_time, @batch_end_time) As NVARCHAR) + 'seconds';
		Print'================================================='
	End Try
	Begin Catch 
	Print '====================================================='
	Print 'Error Occured During Loading Bronze Layer'
	Print 'Error occured'+ Error_Message();
	Print 'Error occured'+ Cast (Error_Number() As NVARCHAR);
	Print 'Error occured'+ Cast (Error_State() As NVARCHAR);
	Print '====================================================='
	End Catch 
End

