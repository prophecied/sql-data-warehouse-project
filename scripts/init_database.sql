

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
