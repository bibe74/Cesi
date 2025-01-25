USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > MacroTipologia
    STAGING_TABLE_NAME > MacroTipologia
*/

/**
 * @table Staging.MacroTipologia
 * @description

 * @depends Import.Libero2MacroTipologia

SELECT TOP 1 * FROM Import.Libero2MacroTipologia;
*/

--DROP TABLE IF EXISTS Staging.MacroTipologia; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Import.Libero2MacroTipologia';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Import.Libero2MacroTipologia')
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
        N'Import.Libero2MacroTipologia',      -- full_table_name - sysname
        N'Staging.MacroTipologia',      -- staging_table_name - sysname
        N'Dim.MacroTipologia',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.MacroTipologiaView', N'V') IS NULL EXEC('CREATE VIEW Staging.MacroTipologiaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.MacroTipologiaView
AS
WITH TableData
AS (
    SELECT DISTINCT
        T.MacroTipologia,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.MacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.IsValidaPerBudgetNuoveVendite,
            T.IsValidaPerBudgetRinnovi,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        T.IsValidaPerBudgetNuoveVendite,
        T.IsValidaPerBudgetRinnovi

    FROM Import.Libero2MacroTipologia T
)
SELECT
    -- Chiavi
    TD.MacroTipologia,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.IsValidaPerBudgetNuoveVendite,
    TD.IsValidaPerBudgetRinnovi

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.MacroTipologia', N'U') IS NOT NULL DROP TABLE Staging.MacroTipologia;
GO

IF OBJECT_ID(N'Staging.MacroTipologia', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.MacroTipologia FROM Staging.MacroTipologiaView;

    ALTER TABLE Staging.MacroTipologia ADD CONSTRAINT PK_Import_MacroTipologia PRIMARY KEY CLUSTERED (UpdateDatetime, MacroTipologia);

    CREATE UNIQUE NONCLUSTERED INDEX IX_MacroTipologia_BusinessKey ON Staging.MacroTipologia (MacroTipologia);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_MacroTipologia', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_MacroTipologia AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_MacroTipologia
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.MacroTipologia';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.MacroTipologia;

    INSERT INTO Staging.MacroTipologia
    SELECT * FROM Staging.MacroTipologiaView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.MacroTipologia;

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

EXEC Staging.usp_Reload_MacroTipologia;
GO

--DROP TABLE IF EXISTS Fact.Scadenze; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Scadenze; DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Dim.MacroTipologia; DROP SEQUENCE IF EXISTS dbo.seq_Dim_MacroTipologia;
GO

IF OBJECT_ID('dbo.seq_Dim_MacroTipologia', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_MacroTipologia START WITH 1;

END;
GO

IF OBJECT_ID('Dim.MacroTipologia', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.MacroTipologia (
        PKMacroTipologia INT NOT NULL CONSTRAINT PK_Dim_MacroTipologia PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_MacroTipologia_PKMacroTipologia DEFAULT (NEXT VALUE FOR dbo.seq_Dim_MacroTipologia),
        MacroTipologia NVARCHAR(60) NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        IsValidaPerBudgetNuoveVendite BIT NOT NULL,
        IsValidaPerBudgetRinnovi BIT NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_MacroTipologia_id_MacroTipologia ON Dim.MacroTipologia (MacroTipologia);
    
    ALTER TABLE Dim.MacroTipologia ADD CONSTRAINT DFT_Dim_MacroTipologia_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.MacroTipologia ADD CONSTRAINT DFT_Dim_MacroTipologia_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.MacroTipologia ADD CONSTRAINT DFT_Dim_MacroTipologia_IsDeleted DEFAULT (0) FOR IsDeleted;

    INSERT INTO Dim.MacroTipologia (
        PKMacroTipologia,
        MacroTipologia,
        IsValidaPerBudgetNuoveVendite,
        IsValidaPerBudgetRinnovi
    )
    VALUES
    (   -1,         -- PKMacroTipologia - int
        N'',        -- MacroTipologia - nvarchar(60)
        0,          -- IsValidaPerBudgetNuoveVendite - bit
        0           -- IsValidaPerBudgetRinnovi - bit
    );--,
    --(   -101,       -- PKMacroTipologia - int
    --    N'<???>',   -- MacroTipologia - nvarchar(60)
    --    0,          -- IsValidaPerBudgetNuoveVendite - bit
    --    0           -- IsValidaPerBudgetRinnovi - bit
    --);

    ALTER SEQUENCE dbo.seq_Dim_MacroTipologia RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_MacroTipologia', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_MacroTipologia AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_MacroTipologia
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.MacroTipologia';

    MERGE INTO Dim.MacroTipologia AS TGT
    USING Staging.MacroTipologia (nolock) AS SRC
    ON SRC.MacroTipologia = TGT.MacroTipologia

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IsValidaPerBudgetNuoveVendite = SRC.IsValidaPerBudgetNuoveVendite,
        TGT.IsValidaPerBudgetRinnovi = SRC.IsValidaPerBudgetRinnovi

    WHEN NOT MATCHED
      THEN INSERT (
        MacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IsValidaPerBudgetNuoveVendite,
        IsValidaPerBudgetRinnovi
      )
      VALUES (
        SRC.MacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IsValidaPerBudgetNuoveVendite,
        SRC.IsValidaPerBudgetRinnovi
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.MacroTipologia' AS full_olap_table_name,
        'MacroTipologia = ' + CAST(COALESCE(inserted.MacroTipologia, deleted.MacroTipologia) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.MacroTipologia
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_MacroTipologia;
GO
