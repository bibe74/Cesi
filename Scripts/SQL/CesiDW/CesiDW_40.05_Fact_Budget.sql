USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > Import
    TABLE_NAME > Budget
    STAGING_TABLE_NAME > Budget
*/

/**
 * @table Staging.Budget
 * @description

 * @depends Import.Budget

SELECT TOP 1 * FROM Import.Budget;
*/

--DROP TABLE IF EXISTS Staging.Budget; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Import.Budget';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Import.Budget')
BEGIN

    INSERT INTO audit.tables (
        provider_name,
        full_table_name,
        staging_table_name,
        datawarehouse_table_name,
        lastupdated_staging,
        lastupdated_local
    )
    VALUES
    (   N'MyDatamartReporting',       -- provider_name - nvarchar(60)
        N'Import.Budget',      -- full_table_name - sysname
        N'Staging.Budget',      -- staging_table_name - sysname
        N'Fact.Budget',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.BudgetView', N'V') IS NULL EXEC('CREATE VIEW Staging.BudgetView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.BudgetView
AS
WITH TableData
AS (
    SELECT
        DIM.PKData,
        CA.PKCapoArea,
        MT.PKMacroTipologia,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DIM.PKData,
            CA.PKCapoArea,
            MT.PKMacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            B.ImportoBudgetNuoveVendite,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        B.ImportoBudgetNuoveVendite,
        NULL AS ImportoBudgetRinnovi,
        B.ImportoBudgetNuoveVendite AS ImportoBudget

    FROM Import.Budget B
    INNER JOIN Dim.Data DIM ON DIM.PKData = B.PKDataInizioMese
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = B.CapoArea
    INNER JOIN Dim.MacroTipologia MT ON MT.IsValidaPerBudgetNuoveVendite = CAST(1 AS BIT)

    UNION ALL

    SELECT
        DIM.PKData,
        CA.PKCapoArea,
        MT.PKMacroTipologia,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DIM.PKData,
            CA.PKCapoArea,
            MT.PKMacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            B.ImportoBudgetNuoveVendite,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        NULL AS ImportoBudgetNuoveVendite,
        B.ImportoBudgetRinnovi,
        B.ImportoBudgetRinnovi AS ImportoBudget

    FROM Import.Budget B
    INNER JOIN Dim.Data DIM ON DIM.PKData = B.PKDataInizioMese
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = B.CapoArea
    INNER JOIN Dim.MacroTipologia MT ON MT.IsValidaPerBudgetRinnovi = CAST(1 AS BIT)
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCapoArea,
    TD.PKMacroTipologia,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Misure
    TD.ImportoBudgetNuoveVendite,
    TD.ImportoBudgetRinnovi,
    TD.ImportoBudget

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Budget', N'U') IS NOT NULL DROP TABLE Staging.Budget; UPDATE audit.tables SET lastupdated_staging = NULL, lastupdated_local = NULL WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Import.Budget';
GO

IF OBJECT_ID(N'Staging.Budget', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Budget FROM Staging.BudgetView;

    ALTER TABLE Staging.Budget ADD CONSTRAINT PK_Staging_Budget PRIMARY KEY CLUSTERED (UpdateDatetime, PKData, PKCapoArea, PKMacroTipologia);

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Budget_BusinessKey ON Staging.Budget (PKData, PKCapoArea, PKMacroTipologia);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Budget', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Budget AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Budget
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.Budget';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Budget;

    INSERT INTO Staging.Budget
    SELECT * FROM Staging.BudgetView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Budget;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

END;
GO

EXEC Staging.usp_Reload_Budget;
GO

--DROP TABLE IF EXISTS Fact.Budget; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Budget;
GO

IF OBJECT_ID('dbo.seq_Fact_Budget', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Budget START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Budget', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Budget (
        PKBudget INT NOT NULL CONSTRAINT PK_Fact_Budget PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Budget_PKBudget DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Budget),

        PKData DATE NOT NULL CONSTRAINT FK_Fact_Budget_PKData FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKCapoArea INT NOT NULL CONSTRAINT FK_Fact_Budget_PKCapoArea FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea),
        PKMacroTipologia INT NOT NULL CONSTRAINT FK_Fact_Budget_PKMacroTipologia FOREIGN KEY REFERENCES Dim.MacroTipologia (PKMacroTipologia),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

    	ImportoBudgetNuoveVendite DECIMAL(10, 2) NULL,
    	ImportoBudgetRinnovi DECIMAL(10, 2) NULL,
    	ImportoBudget DECIMAL(10, 2) NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Budget_PKData_PKCapoArea_PKMacroTipologia ON Fact.Budget (PKData, PKCapoArea, PKMacroTipologia);

    ALTER SEQUENCE dbo.seq_Fact_Budget RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Budget', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Budget AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Budget
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.Budget';

    MERGE INTO Fact.Budget AS TGT
    USING Staging.Budget (nolock) AS SRC
    ON SRC.PKData = TGT.PKData AND SRC.PKCapoArea = TGT.PKCapoArea AND SRC.PKMacroTipologia = TGT.PKMacroTipologia

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.ImportoBudgetNuoveVendite = SRC.ImportoBudgetNuoveVendite,
        TGT.ImportoBudgetRinnovi = SRC.ImportoBudgetRinnovi,
        TGT.ImportoBudget = SRC.ImportoBudget

    WHEN NOT MATCHED
      THEN INSERT (
        PKData,
        PKCapoArea,
        PKMacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        ImportoBudgetNuoveVendite,
        ImportoBudgetRinnovi,
        ImportoBudget
      )
      VALUES (
        SRC.PKData,
        SRC.PKCapoArea,
        SRC.PKMacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.ImportoBudgetNuoveVendite,
        SRC.ImportoBudgetRinnovi,
        SRC.ImportoBudget
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Budget' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.PKData, deleted.PKData) AS NVARCHAR(1000)) + ', PKCapoArea = ' + CAST(COALESCE(inserted.PKCapoArea, deleted.PKCapoArea) AS NVARCHAR(1000)) + ', PKMacroTipologia = ' + CAST(COALESCE(inserted.PKMacroTipologia, deleted.PKMacroTipologia) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Budget
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Budget;
GO
