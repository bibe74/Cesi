USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Gruppo_Agenti
    STAGING_TABLE_NAME > GruppoAgenti
*/

/**
 * @table Staging.GruppoAgenti
 * @description

 * @depends Landing.COMETA_Gruppo_Agenti

SELECT TOP 1 * FROM Landing.COMETA_Gruppo_Agenti;
*/

--DROP TABLE IF EXISTS Staging.GruppoAgenti; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Gruppo_Agenti';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Gruppo_Agenti')
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
        N'Landing.COMETA_Gruppo_Agenti',      -- full_table_name - sysname
        N'Staging.GruppoAgenti',      -- staging_table_name - sysname
        N'Dim.GruppoAgenti',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.GruppoAgentiView', N'V') IS NULL EXEC('CREATE VIEW Staging.GruppoAgentiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.GruppoAgentiView
AS
WITH TableData
AS (
    SELECT
        GA.id_gruppo_agenti,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.id_gruppo_agenti,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.codice,
            GA.descrizione,
            GA.id_sog_com_capo_area,
            ACA.rag_soc_1,
            GA.id_sog_com_agente,
            AA.rag_soc_1,
            GA.id_sog_com_sub_agente,
            ASA.rag_soc_1,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        GA.codice AS IDGruppoAgenti,
        GA.descrizione AS GruppoAgenti,
        GA.id_sog_com_capo_area AS IDSoggettoCommercialeCapoArea,
        COALESCE(ACA.rag_soc_1, CASE WHEN GA.id_sog_com_capo_area IS NULL THEN N'' ELSE N'<???>' END) AS CapoArea,
        GA.id_sog_com_agente AS IDSoggettoCommercialeAgente,
        COALESCE(AA.rag_soc_1, CASE WHEN GA.id_sog_com_agente IS NULL THEN N'' ELSE N'<???>' END) AS Agente,
        GA.id_sog_com_sub_agente AS IDSoggettoCommercialeSubagente,
        COALESCE(ASA.rag_soc_1, CASE WHEN GA.id_sog_com_sub_agente IS NULL THEN N'' ELSE N'<???>' END) AS Subagente

    FROM Landing.COMETA_Gruppo_Agenti GA
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCCA ON SCCA.id_sog_commerciale = GA.id_sog_com_capo_area
    LEFT JOIN Landing.COMETA_Anagrafica ACA ON ACA.id_anagrafica = SCCA.id_anagrafica
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCA ON SCA.id_sog_commerciale = GA.id_sog_com_agente
    LEFT JOIN Landing.COMETA_Anagrafica AA ON AA.id_anagrafica = SCA.id_anagrafica
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCSA ON SCSA.id_sog_commerciale = GA.id_sog_com_sub_agente
    LEFT JOIN Landing.COMETA_Anagrafica ASA ON ASA.id_anagrafica = SCSA.id_anagrafica
)
SELECT
    -- Chiavi
    TD.id_gruppo_agenti,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.IDGruppoAgenti,
    TD.GruppoAgenti,
    --TD.IDSoggettoCommercialeCapoArea,
    TD.CapoArea,
    --TD.IDSoggettoCommercialeAgente,
    TD.Agente,
    --TD.IDSoggettoCommercialeSubagente,
    TD.Subagente

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.GruppoAgenti', N'U') IS NOT NULL DROP TABLE Staging.GruppoAgenti; UPDATE audit.tables SET lastupdated_staging = NULL, lastupdated_local = NULL WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Gruppo_Agenti';
GO

IF OBJECT_ID(N'Staging.GruppoAgenti', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.GruppoAgenti FROM Staging.GruppoAgentiView;

    ALTER TABLE Staging.GruppoAgenti ADD CONSTRAINT PK_Staging_Gruppo_Agenti PRIMARY KEY CLUSTERED (UpdateDatetime, id_gruppo_agenti);

    ALTER TABLE Staging.GruppoAgenti ALTER COLUMN IDGruppoAgenti NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.GruppoAgenti ALTER COLUMN GruppoAgenti NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.GruppoAgenti ALTER COLUMN CapoArea NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.GruppoAgenti ALTER COLUMN Agente NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.GruppoAgenti ALTER COLUMN Subagente NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_GruppoAgenti_BusinessKey ON Staging.GruppoAgenti (id_gruppo_agenti);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_GruppoAgenti', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_GruppoAgenti AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_GruppoAgenti
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.GruppoAgenti;

    INSERT INTO Staging.GruppoAgenti
    SELECT * FROM Staging.GruppoAgentiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.GruppoAgenti;

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

EXEC Staging.usp_Reload_GruppoAgenti;
GO

/**
 * @table Staging.CapoArea
 * @description

 * @depends Landing.COMETA_Gruppo_Agenti

SELECT TOP 1 * FROM Landing.COMETA_Gruppo_Agenti;
*/

--DROP TABLE IF EXISTS Staging.CapoArea;
GO

IF OBJECT_ID(N'Staging.CapoAreaView', N'V') IS NULL EXEC('CREATE VIEW Staging.CapoAreaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.CapoAreaView
AS
WITH CapiArea
AS (
    SELECT DISTINCT
        CapoArea

    FROM Staging.GruppoAgenti
),
TableData
AS (
    SELECT
        CA.CapoArea,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CA.CapoArea,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CA.CapoArea,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM CapiArea CA
)
SELECT
    -- Chiavi
    TD.CapoArea,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.CapoArea', N'U') IS NOT NULL DROP TABLE Staging.CapoArea; UPDATE audit.tables SET lastupdated_staging = NULL, lastupdated_local = NULL WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Gruppo_Agenti';
GO

IF OBJECT_ID(N'Staging.CapoArea', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.CapoArea FROM Staging.CapoAreaView;

    ALTER TABLE Staging.CapoArea ADD CONSTRAINT PK_Staging_CapoArea PRIMARY KEY CLUSTERED (UpdateDatetime, CapoArea);

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_CapoArea_BusinessKey ON Staging.CapoArea (CapoArea);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_CapoArea', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_CapoArea AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_CapoArea
AS
BEGIN

    SET NOCOUNT ON;

    --DECLARE @lastupdated_staging DATETIME;
    --DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    --DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    --SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    --FROM audit.tables
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    --IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.CapoArea;

    INSERT INTO Staging.CapoArea
    SELECT * FROM Staging.CapoAreaView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    --SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.CapoArea;

    --IF (@lastupdated_staging IS NOT NULL)
    --BEGIN

    --UPDATE audit.tables
    --SET lastupdated_staging = @lastupdated_staging
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    --END;

    COMMIT

END;
GO

EXEC Staging.usp_Reload_CapoArea;
GO

--DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Fact.Accessi; DROP TABLE IF EXISTS Dim.Cliente; DROP TABLE IF EXISTS Dim.GruppoAgenti; DROP SEQUENCE IF EXISTS dbo.seq_Dim_GruppoAgenti; DROP TABLE IF EXISTS Dim.CapoArea; DROP SEQUENCE IF EXISTS dbo.seq_Dim_CapoArea;
GO

IF OBJECT_ID('dbo.seq_Dim_CapoArea', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_CapoArea START WITH 1;

END;
GO

IF OBJECT_ID('Dim.CapoArea', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.CapoArea (
        PKCapoArea INT NOT NULL CONSTRAINT PK_Dim_CapoArea PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_CapoArea_PKCapoArea DEFAULT (NEXT VALUE FOR dbo.seq_Dim_CapoArea),
        CapoArea NVARCHAR(60) NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_CapoArea_GUIDCapoArea ON Dim.CapoArea (CapoArea);
    
    ALTER TABLE Dim.CapoArea ADD CONSTRAINT DFT_Dim_CapoArea_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.CapoArea ADD CONSTRAINT DFT_Dim_CapoArea_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.CapoArea ADD CONSTRAINT DFT_Dim_CapoArea_IsDeleted DEFAULT (0) FOR IsDeleted;

    INSERT INTO Dim.CapoArea (
        PKCapoArea,
        CapoArea
    )
    VALUES
    (   -1,        -- PKCapoArea - int
        N''        -- CapoArea - nvarchar(60)
    ),
    (   -101,      -- PKCapoArea - int
        N'<???>'   -- CapoArea - nvarchar(60)
    );

    ALTER SEQUENCE dbo.seq_Dim_CapoArea RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_CapoArea', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_CapoArea AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_CapoArea
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    MERGE INTO Dim.CapoArea AS TGT
    USING Staging.CapoArea (NOLOCK) AS SRC
    ON SRC.CapoArea = TGT.CapoArea

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted

    WHEN NOT MATCHED
      THEN INSERT (
        CapoArea,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted
      )
      VALUES (
        SRC.CapoArea,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.CapoArea' AS full_olap_table_name,
        'CapoArea = ' + CAST(COALESCE(inserted.CapoArea, deleted.CapoArea) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.CapoArea
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_CapoArea;
GO

IF OBJECT_ID('dbo.seq_Dim_GruppoAgenti', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_GruppoAgenti START WITH 1;

END;
GO

IF OBJECT_ID('Dim.GruppoAgenti', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.GruppoAgenti (
        PKGruppoAgenti INT NOT NULL CONSTRAINT PK_Dim_GruppoAgenti PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_GruppoAgenti_PKGruppoAgenti DEFAULT (NEXT VALUE FOR dbo.seq_Dim_GruppoAgenti),
        id_gruppo_agenti INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        IDGruppoAgenti NVARCHAR(10) NOT NULL,
        GruppoAgenti NVARCHAR(60) NOT NULL,
        CapoArea NVARCHAR(60) NOT NULL,
        Agente NVARCHAR(60) NOT NULL,
        Subagente NVARCHAR(60) NOT NULL,
        PKCapoArea INT NOT NULL CONSTRAINT FK_Dim_GruppoAgenti_PKCapoArea FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea)
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_GruppoAgenti_GUIDGruppoAgenti ON Dim.GruppoAgenti (id_gruppo_agenti);
    
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_IsDeleted DEFAULT (0) FOR IsDeleted;

    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_IDGruppoAgenti DEFAULT (N'') FOR IDGruppoAgenti;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_GruppoAgenti DEFAULT (N'') FOR GruppoAgenti;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_CapoArea DEFAULT (N'') FOR CapoArea;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_Agente DEFAULT (N'') FOR Agente;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_Subagente DEFAULT (N'') FOR Subagente;
    ALTER TABLE Dim.GruppoAgenti ADD CONSTRAINT DFT_Dim_GruppoAgenti_PKCapoArea DEFAULT (-1) FOR PKCapoArea;

    INSERT INTO Dim.GruppoAgenti (
        PKGruppoAgenti,
        id_gruppo_agenti,
        IDGruppoAgenti,
        GruppoAgenti,
        CapoArea,
        Agente,
        Subagente
    )
    VALUES
    (   -1,        -- PKGruppoAgenti - int
        -1,        -- id_gruppo_agenti - int
        N'',       -- IDGruppoAgenti - nvarchar(10)
        N'',       -- GruppoAgenti - nvarchar(60)
        N'',       -- CapoArea - nvarchar(60)
        N'',       -- Agente - nvarchar(60)
        N''        -- Subagente - nvarchar(60)
    ),
    (   -101,      -- PKGruppoAgenti - int
        -101,      -- id_gruppo_agenti - int
        N'???',    -- IDGruppoAgenti - nvarchar(10)
        N'<???>',  -- GruppoAgenti - nvarchar(60)
        N'<???>',  -- CapoArea - nvarchar(60)
        N'',       -- Agente - nvarchar(60)
        N''        -- Subagente - nvarchar(60)
    );

    ALTER SEQUENCE dbo.seq_Dim_GruppoAgenti RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_GruppoAgenti', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_GruppoAgenti AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_GruppoAgenti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    MERGE INTO Dim.GruppoAgenti AS TGT
    USING Staging.GruppoAgenti (nolock) AS SRC
    ON SRC.id_gruppo_agenti = TGT.id_gruppo_agenti

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IDGruppoAgenti = SRC.IDGruppoAgenti,
        TGT.GruppoAgenti = SRC.GruppoAgenti,
        TGT.CapoArea = SRC.CapoArea,
        TGT.Agente = SRC.Agente,
        TGT.Subagente = SRC.Subagente

    WHEN NOT MATCHED
      THEN INSERT (
        id_gruppo_agenti,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDGruppoAgenti,
        GruppoAgenti,
        CapoArea,
        Agente,
        Subagente
      )
      VALUES (
        SRC.id_gruppo_agenti,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDGruppoAgenti,
        SRC.GruppoAgenti,
        SRC.CapoArea,
        SRC.Agente,
        SRC.Subagente
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.GruppoAgenti' AS full_olap_table_name,
        'id_gruppo_agenti = ' + CAST(COALESCE(inserted.id_gruppo_agenti, deleted.id_gruppo_agenti) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.GruppoAgenti
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE GA
    SET GA.PKCapoArea = CA.PKCapoArea
    FROM Dim.GruppoAgenti GA
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = GA.CapoArea;

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_GruppoAgenti;
GO

/**
 * @table Bridge.ADUserCapoArea
 * @description Tabella di bridge ADUser / CapoArea
*/

CREATE OR ALTER VIEW Bridge.ADUserCapoAreaView
AS
WITH CapiArea
AS (
    SELECT DISTINCT
        CapoArea
    FROM Dim.GruppoAgenti
)
SELECT
    ICA.ADUser,
    CA.CapoArea

FROM CapiArea CA
INNER JOIN Import.CapiArea ICA ON ICA.CapoArea = CA.CapoArea
    AND ICA.ADUser <> N''

UNION

SELECT
    A.ADUser,
    CA.CapoArea

FROM Import.Amministratori A
CROSS JOIN CapiArea CA

UNION

SELECT
    N'CESI\TestAgenti',
    ICA.CapoArea

FROM Import.CapiArea ICA
WHERE ICA.CapoArea = N'ATENEO S.A.S.';
GO

--DROP TABLE IF EXISTS Bridge.ADUserCapoArea;
GO

IF OBJECT_ID('Bridge.ADUserCapoArea', 'U') IS NULL
BEGIN

    SELECT TOP 0 * INTO Bridge.ADUserCapoArea FROM Bridge.ADUserCapoAreaView;

    ALTER TABLE Bridge.ADUserCapoArea ALTER COLUMN ADUser NVARCHAR(60) NOT NULL;
    ALTER TABLE Bridge.ADUserCapoArea ALTER COLUMN CapoArea NVARCHAR(60) NOT NULL;

    ALTER TABLE Bridge.ADUserCapoArea ADD CONSTRAINT PK_Bridge_ADUserCapoArea PRIMARY KEY CLUSTERED (ADUser, CapoArea);

END;
GO

CREATE OR ALTER PROCEDURE Bridge.usp_Merge_ADUserCapoArea
AS
BEGIN

    MERGE INTO Bridge.ADUserCapoArea AS TGT
    USING Bridge.ADUserCapoAreaView AS SRC
    ON SRC.ADUser = TGT.ADUser AND SRC.CapoArea = TGT.CapoArea
    WHEN NOT MATCHED THEN INSERT (
        ADUser,
        CapoArea
    )
    VALUES (
        SRC.ADUser,
        SRC.CapoArea
    )
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Bridge.ADUserCapoArea' AS full_olap_table_name,
        'ADUser = ' + CAST(COALESCE(inserted.ADUser, deleted.ADUser) AS NVARCHAR(1000)) + ', CapoArea = ' + CAST(COALESCE(inserted.CapoArea, deleted.CapoArea) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC Bridge.usp_Merge_ADUserCapoArea;
GO
