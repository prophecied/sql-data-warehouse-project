/*
================================================
Create Database and Schemas
================================================

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the scripts set up three schemas
within the database: 'bronze', 'silver', 'gold'.

WARNING
Running this script will drop the entire 'DataWarehouse' database if it exists.
All the data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running the scripts

*/

Use master;

--Drop and recreate the 'DataWarehouse' database
If Exists (Select 1 From sys.databases Where name = 'DataWarehouse')
Begin
	Alter DATABASE DataWarehouse Set SINGLE_USER With Rollback Immediate;
	Drop Database DataWarehouse;
End;

-- Create Database 'DataWarehouse'
Create DATABASE DataWarehouse;

Use DataWarehouse;

Create Schema bronze; 
Go 

Create Schema silver;
Go

Create Schema gold;
Go 
