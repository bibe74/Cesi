-- [50] Recovery Interval Not Optimal
ALTER DATABASE [dbNormativa] SET TARGET_RECOVERY_TIME = 60 SECONDS;
ALTER DATABASE [MyDatamartReporting] SET TARGET_RECOVERY_TIME = 60 SECONDS;
ALTER DATABASE [MySolution] SET TARGET_RECOVERY_TIME = 60 SECONDS;
ALTER DATABASE [mysolution_it] SET TARGET_RECOVERY_TIME = 60 SECONDS;
GO

-- [50] Instant File Initialization Not Enabled
-- Vedi https://blog.sqlauthority.com/2018/07/31/sql-server-how-to-turn-on-enable-instant-file-initialization/
-- Eseguire le attività indicate per attivare l'instant file initialization

-- [110] Auto-Create Stats Disabled
ALTER DATABASE [dbNormativa] SET AUTO_CREATE_STATISTICS ON;
GO

-- [110] Auto-Update Stats Disabled
ALTER DATABASE [dbNormativa] SET AUTO_UPDATE_STATISTICS ON;
GO

-- [170] Remote DAC Disabled
EXEC sp_configure 'remote admin connections', 1;
GO
RECONFIGURE
GO

-- [200] Backup Compression Default Off
EXEC sp_configure 'backup compression default', 1;  
RECONFIGURE;  
GO

-- [200] cost threshold for parallelism
-- Vedi https://www.brentozar.com/archive/2017/03/why-cost-threshold-for-parallelism-shouldnt-be-set-to-5/
EXEC sp_configure 'show advanced options', 1;  
GO  
RECONFIGURE  
GO  
EXEC sp_configure 'cost threshold for parallelism', 50;  
GO  
RECONFIGURE  
GO

-- [230] Database Owner <> sa
USE dbNormativa; exec sp_changedbowner [sa]; -- attualmente il db owner è DB2021\Administrator
USE MyDatamartReporting; exec sp_changedbowner [sa]; -- attualmente il db owner è DB2021\Administrator
USE MySolution; exec sp_changedbowner [sa]; -- attualmente il db owner è DB2021\Administrator
USE mysolution_it; exec sp_changedbowner [sa]; -- attualmente il db owner è DB2021\Administrator
USE Nop_MySolution; exec sp_changedbowner [sa]; -- attualmente il db owner è oeds. Verificare se è strettamente necessario.
USE master;
GO

-- [230] Jobs Owned By Users
exec msdb..sp_update_job
    @job_name = 'Backup DB.Subplan_1',
    @owner_login_name = 'sa';
GO
