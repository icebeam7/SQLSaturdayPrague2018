-- Check install
USE tpcxbb_1gb;
GO
SELECT TOP (100) * FROM [dbo].[store_sales];
SELECT TOP (100) * FROM [dbo].[store_returns];