-- Enable external script execution
EXEC sp_configure 'external scripts enabled', 1;
GO
RECONFIGURE WITH OVERRIDE
GO

-- Install and configure R development environment

-- Download Database backup from https://sqlchoice.blob.core.windows.net/sqlchoice/static/tpcxbb_1gb.bak
-- Save it to C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\Backup\tpcxbb_1gb.bak

-- Restore the database
USE master;
GO
RESTORE DATABASE tpcxbb_1gb
   FROM DISK = 'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\Backup\tpcxbb_1gb.bak'
   WITH
	MOVE 'tpcxbb_1gb' TO 'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\DATA\tpcxbb_1gb.mdf'
	,MOVE 'tpcxbb_1gb_log' TO 'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\DATA\tpcxbb_1gb.ldf';
GO

