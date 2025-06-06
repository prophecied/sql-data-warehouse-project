/*
======================================================================
Quality Checks
======================================================================
Script Purpose:
	This script performs various quality checks for data consistency,
	accuracy and standardization across 'silver' schemas. It includes
	checks for:
	- Null or duplicate primary keys
	- Unwanted spaces in string fields
	- Data standardization and consistency
	- Invalid date ranges and orders
	- Data consistency between related fields

Usage Notes:
	- Run these checks after data loading Silver Layer
	- Investigate and resolve any discrepancies found during the checks 
*/

-- ===================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================
-- Check for Nulls or Duplicates in Primary Key 
-- Expectation: No Result 
Select 
cst_id,
Count(*)
From silver.crm_cust_info
Group By cst_id
Having Count(*) > 1 or cst_id Is Null

-- Check for unwanted spaces
-- Expectation: No Result 
Select cst_firstname
From silver.crm_cust_info
WHERE cst_firstname != Trim(cst_firstname)

--Data Standardization and Consistency
Select Distinct cst_gndr
From silver.crm_cust_info

Select Distinct cst_marital_status
From silver.crm_cust_info

-- ===================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================
--Check for Nulls or Duplicates in Primary Key 
--Expectation: No Result 
Select 
prd_id,
count(*)
From silver.crm_prd_info
Group by prd_id
Having Count(*)> 1 or prd_id is Null 

--Check for unwanted spaces 
-- Expectation: No Results 
Select prd_nm 
From silver.crm_prd_info 
Where prd_nm != Trim(prd_nm)

--Check for Nulls or Negative Numbers 
--Expectations: No Results 

Select prd_cost
From silver.crm_prd_info 
Where prd_cost < 0 or prd_cost is Null 

--Data Standardization & Consistency 
Select Distinct prd_line 
From silver.crm_prd_info 

--Check for Invalid Date Orders
Select *
From silver.crm_prd_info 
where prd_end_dt < prd_start_dt

-- ===================================================================
-- Checking 'silver.crm_sales_details'
-- ===================================================================
--Check for Invalids Dates
Select 
Nullif(sls_due_dt,0) sls_due_dt
From silver.crm_sales_details 
Where sls_due_dt <= 0 
or Len(sls_due_dt) != 8 
or sls_due_dt > 20500101 
or sls_due_dt < 19000101

--Check for Invalid Date Orders
Select *
From silver.crm_sales_details 
Where sls_order_dt > sls_ship_dt Or sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity and Price 
--Sales = Quantity * Price
--Values must not be Null, zero or negative

Select Distinct
sls_sales,
sls_quantity,
sls_price
From silver.crm_sales_details 
Where sls_sales != sls_quantity * sls_price
Or sls_sales Is Null Or sls_quantity Is Null Or sls_price Is Null 
Or sls_sales <= 0 Or sls_quantity <= 0 or sls_price <=0
Order By sls_sales, sls_quantity, sls_price

-- ===================================================================
-- Checking 'silver.erp_loc_a101'
-- ===================================================================
--Data Standardization & Consistency 
Select Distinct cntry
From silver.erp_loc_a101
Order By cntry 

-- ===================================================================
-- Checking 'silver.erp_cust_az12'
-- ===================================================================
-- Identify Out of Range Dates
Select Distinct 
bdate
From silver.erp_cust_az12
Where bdate < '1924-01-01' Or bdate > Getdate()

--Data Standardization & Consistency 
Select Distinct 
gen,
Case When Upper(Trim(gen)) In ('F', 'FEMALE') Then 'Female' 
	 When Upper(Trim(gen)) In ('M', 'MALE') Then 'Male'
	 Else 'n/a'
End As gen 
From silver.erp_cust_az12

-- ===================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ===================================================================
-- Check unwanted spaces
Select * From silver.erp_px_cat_g1v2
where cat! = Trim(cat) or subcat!= Trim(subcat) or maintenance != Trim(maintenance)

--Check Standardization & Consistency 
Select Distinct 
maintenance
From silver.erp_px_cat_g1v2

