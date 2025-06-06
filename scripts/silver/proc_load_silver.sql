/*
=========================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==========================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load)
  process to populate the 'silver' schema tables from the 'bronze' schema
Actions Performed: 
  - Truncate Silver tables.
  - Inserts transformed and cleanes data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values 

Usage Example 
  Exec silver.load_silver
============================================================================
*/

Create Or Alter Procedure silver.load_silver As
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin Try
		Set @batch_start_time = Getdate();
		Print '======================================================';
		Print 'Loading Silver Layer';
		Print '======================================================';

		Print '------------------------------------------------------';
		Print 'Loading CRM Table';
		Print '------------------------------------------------------';

	--Loading silver.crm_cust_info
	Set @start_time = GETDATE();
	Print '>> Truncating Table: silver.crm_cust_info';
	Truncate Table silver.crm_cust_info;
	Print'>> Inserting Data Into: silver.crm_cust_info';
	Insert Into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
	Select 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	Case When upper(Trim(cst_marital_status)) = 'S' then 'Single'
		When Upper(Trim(cst_marital_status)) = 'M' then 'Married'
		Else 'n/a'
	End cst_marital_status,
	Case When upper(Trim(cst_gndr)) = 'F' then 'Female'
		When Upper(Trim(cst_gndr)) = 'M' then 'Male'
		Else 'n/a'
	End cst_gndr,
	cst_create_date
	From(
		Select 
		*,
		ROW_NUMBER() Over (Partition by cst_id Order by cst_create_date Desc) as flag_last
		From bronze.crm_cust_info
		where cst_id is not Null 
	)t Where flag_last = 1
	Set @end_time = GETDATE();
	Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
	Print'>>---------------';

	--Loading silver.crm_prd_info
	Set @start_time = GETDATE()
	Print '>> Truncating Table: silver.crm_prd_info';
	Truncate Table silver.crm_prd_info;
	Print'>> Inserting Data Into: silver.crm_prd_info';
	Insert Into silver.crm_prd_info(
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
	Replace(Substring(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID
	Substring(prd_key, 7, Len(prd_key)) as prd_key, --Extract product key 
	prd_nm,
	Isnull(prd_cost, 0) as prd_cost,
	Case Upper(Trim(prd_line))
		 When 'M' Then 'Mountain'
		 When 'R' Then 'Road'
		 When 'S' Then 'Other Sales'
		 When 'T' Then 'Touring'
		 Else 'n/a'
	End as prd_line, --Map product line codes to descriptive values
	Cast(prd_start_dt as Date) as prd_Start_dt,
	Cast(Lead(prd_start_dt) Over(Partition by prd_key Order by prd_start_dt)-1 
		 as Date
		 ) as prd_end_dt --Calculate end date as one day before the next start date 
	From bronze.crm_prd_info
	Set @end_time = GETDATE();
	Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
	Print'>>---------------';

	--Loading silver.crm_sales_details
	Set @start_time = GETDATE()
	Print '>> Truncating Table: silver.crm_sales_details';
	Truncate Table silver.crm_sales_details;
	Print'>> Inserting Data Into: silver.crm_sales_details';
	Insert Into silver.crm_sales_details (
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
	Case When sls_order_dt = 0 Or Len(sls_order_dt) != 8 Then Null 
		 Else Cast(Cast(sls_order_dt As Varchar) As Date)
	End As sls_order_dt,
	Case When sls_ship_dt= 0 Or Len(sls_ship_dt) != 8 Then Null 
		 Else Cast(Cast(sls_ship_dt As Varchar) As Date)
	End As sls_ship_dt,
	Case When sls_due_dt= 0 Or Len(sls_due_dt) != 8 Then Null 
		 Else Cast(Cast(sls_due_dt As Varchar) As Date)
	End As sls_due_dt,
	Case When sls_sales Is Null Or sls_Sales <= 0 Or sls_sales != Sls_quantity * Abs(sls_price)
			Then sls_quantity * Abs(sls_price)
		Else  sls_sales
	End As sls_sales, --Recalculate sales if original value is missing or incorrect
	sls_quantity,
	Case When sls_price Is Null or sls_price <= 0
			Then sls_sales / Nullif(sls_quantity, 0 )
		Else sls_price
	End As sls_price
	From bronze.crm_sales_details 
	Set @end_time = GETDATE();
	Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
	Print'>>---------------';

	--Loading silver.erp_loc_a101
	Set @start_time = GETDATE()
	Print '>> Truncating Table: silver.erp_loc_a101';
	Truncate Table silver.erp_loc_a101;
	Print '>> Inserting Data Info: silver.erp_px_cat_g1v2';
	Insert Into silver.erp_loc_a101
	(cid,cntry)
	Select 
	Replace(cid, '-', '') cid,
	Case When Trim(cntry) = 'DE' Then 'Germany'
		 When Trim(cntry) In ('US', 'USA') Then 'United States'
		 When Trim(cntry) = '' Or cntry Is Null Then 'n/a'
		 Else(cntry)
	End As cntry --Normalize and Handle missing or blank country codes 
	From bronze.erp_loc_a101
	Set @end_time = GETDATE();
	Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
	Print'>>---------------';

	--Loading silver.erp_cust_az12
	Set @start_time = GETDATE()
	Print '>> Truncating Table: silver.erp_cust_az12';
	Truncate Table silver.erp_cust_az12;
	Print '>>Inserting Data Into: silver.erp_cust_az12';
	Insert Into silver.erp_cust_az12(cid, bdate, gen)
	Select 
	Case When cid Like 'NAS%' Then Substring(cid, 4, Len(cid)) 
		 Else cid 
	End cid,
	Case When bdate > Getdate() Then Null 
		 Else bdate
	End As bdate,
	Case When Upper(Trim(gen)) In ('F', 'FEMALE') Then 'Female' 
		 When Upper(Trim(gen)) In ('M', 'MALE') Then 'Male'
		 Else 'n/a'
	End As gen 
	From bronze.erp_cust_az12
	Set @end_time = GETDATE();
	Print'>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) As NVARCHAR) + 'seconds';
	Print'>>---------------';

	--Loading silver.erp_px_catg1v2
	Set @start_time = GETDATE()
	Print '>>Truncating Table: silver.erp_px_catg1v2';
	Truncate Table silver.erp_px_cat_g1v2;
	Print'>> Inserting Data Into: silver.erp_px_cat_g1v2';
	Insert Into silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	Select 
	id, 
	cat,
	subcat,
	maintenance
	From bronze.erp_px_cat_g1v2
	Set @end_time = GETDATE();
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
