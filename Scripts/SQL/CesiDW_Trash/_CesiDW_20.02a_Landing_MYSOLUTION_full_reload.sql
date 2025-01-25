USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/* Reload Landing.MYSOLUTION_InfoAccounts - BEGIN */ 

TRUNCATE TABLE Landing.MYSOLUTION_InfoAccounts;
GO

-- dbo.InfoAccounts_overall_001
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_overall_001;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts_overall
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_overall;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts_20200101
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_20200101;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts_20200102
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_20200102;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts_20200908
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_20200908;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts_20200909
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts_20200909;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

-- dbo.InfoAccounts
DROP SYNONYM MYSOLUTION.InfoAccounts;
CREATE SYNONYM MYSOLUTION.InfoAccounts FOR MyDatamartReporting.dbo.InfoAccounts;
GO

EXEC MYSOLUTION.usp_Merge_InfoAccounts;
GO

UPDATE T
SET T.IsDeleted = V.IsDeleted
FROM Landing.MYSOLUTION_InfoAccounts T
INNER JOIN Landing.MYSOLUTION_InfoAccountsView V ON V.guid_account = T.guid_account
WHERE T.IsDeleted <> V.IsDeleted;
GO

/* Reload Landing.MYSOLUTION_InfoAccounts - END */ 
