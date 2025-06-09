/*
================================================================================
DDL Script: Create Gold Views
================================================================================
Script Purpose:
	This script creates views for the Gold layer in the data warehouse.
	The Gold layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver layer
	to produce a clean, enriched, and business- ready dataset. 

Usage: 
	- These views can be queried directly for analytics and reporting. 
================================================================================
*/

--==============================================================================
-- Create Dimension: gold.dim_customers
--==============================================================================
If Object_Id('gold.dim_customers', 'V') Is Not Null
	Drop View gold.dim_customers;
Go

Create View gold.dim_customers As
Select 
	Row_Number() Over (Order By cst_id) as customer_key,--Surrogate key
	ci.cst_id As customer_id,
	ci.cst_key As customer_number,
	ci.cst_firstname As first_name,
	ci.cst_lastname As last_name,
	la.cntry As country,
	ci.cst_marital_status As marital_status,
	Case When ci.cst_gndr != 'n/a' Then ci.cst_gndr --CRM is the Master for gender Info
		Else Coalesce(ca.gen, 'n/a')                --Fallback to ERP data
	End As gender,
	ca.bdate As birthdate,
	ci.cst_create_date As create_date		
	From silver.crm_cust_info ci
	Left Join silver.erp_cust_az12 ca
	On		  ci.cst_key = ca.cid
	Left Join silver.erp_loc_a101 la 
	On		  ci.cst_key = la.cid
Go
--================================================================================
-- Create Dimension: gold.dim_products
--================================================================================
If Object_Id('gold.dim_products', 'V') Is Not Null
	Drop View gold.dim_products;
Go

Create View gold.dim_products As 
Select 
Row_number() Over (Order By pn.prd_start_dt, pn.prd_key) as product_key, --Surrogate key
pn.prd_id As product_id,
pn.prd_key As product_number,
pn.prd_nm As product_name,
pn.cat_id As category_id,
pc.cat As category,
pc.subcat As subcategory,
pn.prd_cost As cost,
pn.prd_line As product_line,
pn.prd_start_dt As start_date,
pc.maintenance
From silver.crm_prd_info pn 
Left Join silver.erp_px_cat_g1v2 pc 
On pn.cat_id = pc.id 
Where prd_end_dt Is Null --Filter out all historical data 
Go

--====================================================================================
-- Create Fact Table: gold.fact_sales
--====================================================================================
If Object_Id('gold.fact_sales', 'V') Is Not Null
	Drop View gold.fact_sales;
Go

Create View gold.fact_sales As
Select 
sd.sls_ord_num As order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt As order_date,
sd.sls_ship_dt As shipping_date,
sd.sls_due_dt As due_date,
sd.sls_sales As sales_amount,
sd.sls_quantity As quantity,
sd.sls_price As price
From silver.crm_sales_details sd
Left Join gold.dim_products pr 
On sd.sls_prd_key = pr.product_number
Left Join gold.dim_customers cu
On sd.sls_cust_id = cu.customer_id
Go
