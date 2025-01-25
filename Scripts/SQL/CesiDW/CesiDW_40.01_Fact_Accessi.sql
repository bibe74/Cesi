USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > LogsForReport
*/

/**
 * @table Staging.Accessi
 * @description

 * @depends Landing.MYSOLUTION_LogsForReport

SELECT TOP 1 * FROM Landing.MYSOLUTION_LogsForReport;
*/

--DROP TABLE IF EXISTS Staging.Accessi; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_LogsForReport';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_LogsForReport')
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
        N'Landing.MYSOLUTION_LogsForReport',      -- full_table_name - sysname
        N'Staging.Accessi',      -- staging_table_name - sysname
        N'Fact.Accessi',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.AccessiView', N'V') IS NULL EXEC('CREATE VIEW Staging.AccessiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.AccessiView
AS
WITH UtentiConPagineVisitate
AS (
    SELECT DISTINCT Username
    FROM Landing.MYSOLUTION_LogsForReport LFR
),
UtentiConPagineVisitateClienti
AS (
    SELECT
        UCPV.Username,
        C.PKCliente

    FROM UtentiConPagineVisitate UCPV
    INNER JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCPV.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale

    UNION ALL

    SELECT
        UCPV.Username,
        C.PKCliente

    FROM UtentiConPagineVisitate UCPV
    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCPV.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    INNER JOIN Dim.Cliente C ON C.Email = UCPV.Username
        AND C.HasAnagraficaCometa = CAST(0 AS BIT)
    WHERE SCE.Email IS NULL
),
AccessiDettaglio
AS (
    SELECT
        --LFR.Data,
        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKData,
        --LFR.IDUser,
        COALESCE(UCAC.PKCliente, -101) AS PKCliente,
        LFR.NumeroAccessi,
        LFR.NumeroPagineVisitate

    FROM Landing.MYSOLUTION_LogsForReport LFR
    LEFT JOIN Dim.Data D ON D.PKData = LFR.Data
    LEFT JOIN UtentiConPagineVisitateClienti UCAC ON UCAC.Username = LFR.Username
    WHERE LFR.IsDeleted = CAST(0 AS BIT)
        AND LFR.Username <> N''
),
TableData
AS (
    SELECT
        AD.PKData,
        AD.PKCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.PKData,
            AD.PKCliente,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.PKCapoArea,
            SUM(AD.NumeroAccessi),
            SUM(AD.NumeroPagineVisitate),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        GA.PKCapoArea,
        SUM(AD.NumeroAccessi) AS NumeroAccessi,
        SUM(AD.NumeroPagineVisitate) AS NumeroPagineVisitate

    FROM AccessiDettaglio AD
    INNER JOIN Dim.Cliente C ON C.PKCliente = AD.PKCliente
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
    GROUP BY AD.PKData,
        AD.PKCliente,
        GA.PKCapoArea
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Dimensioni
    TD.PKCapoArea,

    -- Misure
    TD.NumeroAccessi,
    TD.NumeroPagineVisitate

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Accessi', N'U') IS NOT NULL DROP TABLE Staging.Accessi;
GO

IF OBJECT_ID(N'Staging.Accessi', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Accessi FROM Staging.AccessiView;

    ALTER TABLE Staging.Accessi ALTER COLUMN PKData DATE NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN PKCliente INT NOT NULL;

    ALTER TABLE Staging.Accessi ADD CONSTRAINT PK_Staging_Accessi PRIMARY KEY CLUSTERED (PKData, PKCliente);

    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroAccessi INT NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroPagineVisitate INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Accessi_PKCliente_PKData ON Staging.Accessi (PKCliente, PKData);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Accessi', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Accessi AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_LogsForReport';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Accessi;

    INSERT INTO Staging.Accessi
    SELECT * FROM Staging.AccessiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Accessi;

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

EXEC Staging.usp_Reload_Accessi;
GO

--DROP TABLE IF EXISTS Fact.Accessi; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Accessi;
GO

IF OBJECT_ID('dbo.seq_Fact_Accessi', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Accessi START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Accessi', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Accessi (
        PKAccessi INT NOT NULL CONSTRAINT PK_Fact_Accessi PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Accessi_PKAccessi DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Accessi),
        PKData DATE NOT NULL CONSTRAINT FK_Fact_Accessi_PKData FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKCliente INT NOT NULL CONSTRAINT FK_Fact_Accessi_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        NumeroAccessi INT NOT NULL,
        NumeroPagineVisitate INT NOT NULL,
        PKCapoArea INT NOT NULL CONSTRAINT FK_Fact_Accessi_PKCapoArea FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea)
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Accessi_PKData_PKCliente ON Fact.Accessi (PKData, PKCliente);

    ALTER SEQUENCE dbo.seq_Fact_Accessi RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Accessi', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Accessi AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_LogsForReport';

    MERGE INTO Fact.Accessi AS TGT
    USING Staging.Accessi (nolock) AS SRC
    ON SRC.PKData = TGT.PKData AND SRC.PKCliente = TGT.PKCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.NumeroAccessi = SRC.NumeroAccessi,
        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate,
        TGT.PKCapoArea = SRC.PKCapoArea

    WHEN NOT MATCHED
      THEN INSERT (
        PKData,
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NumeroAccessi,
        NumeroPagineVisitate,
        PKCapoArea
      )
      VALUES (
        SRC.PKData,
        SRC.PKCliente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.NumeroAccessi,
        SRC.NumeroPagineVisitate,
        SRC.PKCapoArea
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Accessi' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.PKData, deleted.PKData) AS NVARCHAR(1000)) + ', PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Accessi
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Accessi;
GO
