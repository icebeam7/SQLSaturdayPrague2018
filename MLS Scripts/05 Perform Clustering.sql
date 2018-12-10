-- PERFORM CLUSTERING
USE [tpcxbb_1gb]

--Creating a table for storing the clustering data
DROP TABLE IF EXISTS [dbo].[py_customer_clusters];
GO
--Create a table to store the predictions in
CREATE TABLE [dbo].[py_customer_clusters](
 [Customer] [bigint] NULL,
 [OrderRatio] [float] NULL,
 [itemsRatio] [float] NULL,
 [monetaryRatio] [float] NULL,
 [frequency] [float] NULL,
 [cluster] [int] NULL,
 ) ON [PRIMARY]
GO

--Execute the clustering and insert results into table
INSERT INTO py_customer_clusters
EXEC [dbo].[py_generate_customer_return_clusters];

-- Select contents of the table
SELECT * FROM py_customer_clusters;

