/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three 
    schemas: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- Drop and recreate the 'DataWarehouse' database
DROP DATABASE IF EXISTS datawarehouse;


-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;


USE DataWarehouse;


-- Create Schemas
DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;

