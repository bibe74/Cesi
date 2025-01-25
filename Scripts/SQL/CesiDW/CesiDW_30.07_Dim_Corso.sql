USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > WEBINARS
    TABLE_NAME > WeBinars
    STAGING_TABLE_NAME > Corso
*/

/**
 * @table Staging.Corso
 * @description

 * @depends Landing.WEBINARS_WeBinars

SELECT TOP 1 * FROM Landing.WEBINARS_WeBinars;
*/

--DROP TABLE IF EXISTS Staging.Corso; DELETE FROM audit.tables WHERE provider_name = N'Webinars' AND full_table_name = N'Landing.WEBINARS_WeBinars';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'Webinars' AND full_table_name = N'Landing.WEBINARS_WeBinars')
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
    (   N'Webinars',       -- provider_name - nvarchar(60)
        N'Landing.WEBINARS_WeBinars',      -- full_table_name - sysname
        N'Staging.Corso',      -- staging_table_name - sysname
        N'Dim.Corso',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.CorsoView', N'V') IS NULL EXEC('CREATE VIEW Staging.CorsoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.CorsoView
AS
WITH TableData
AS (
    SELECT
        W.Source AS IDCorso,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            W.Source,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            W.CourseTitle,
            W.CourseType,
            W.VideoTitle,
            W.VideoStartDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        W.CourseTitle AS Corso,
        W.CourseType AS TipoCorso,
        W.VideoTitle AS Giornata,
        CONVERT(DATE, W.VideoStartDate) AS DataInizioCorso,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioCorso,
        FORMAT(W.VideoStartDate, 'HH:mm') AS OraInizioCorso

    FROM Landing.WEBINARS_WeBinars W
    LEFT JOIN Dim.Data DIC ON DIC.PKData = CONVERT(DATE, W.VideoStartDate)
    WHERE W.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.IDCorso,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Corso,
    TD.TipoCorso,
    TD.Giornata,
    TD.PKDataInizioCorso,
    TD.OraInizioCorso

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Corso', N'U') IS NOT NULL DROP TABLE Staging.Corso;
GO

IF OBJECT_ID(N'Staging.Corso', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Corso FROM Staging.CorsoView;

    ALTER TABLE Staging.Corso ADD CONSTRAINT PK_Landing_WEBINARS_WeBinars PRIMARY KEY CLUSTERED (UpdateDatetime, IDCorso);

    ALTER TABLE Staging.Corso ALTER COLUMN PKDataInizioCorso DATE NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_WeBinars_BusinessKey ON Staging.Corso (IDCorso);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Corso', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Corso AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Corso
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Landing.WEBINARS_WeBinars';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Corso;

    INSERT INTO Staging.Corso
    SELECT * FROM Staging.CorsoView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Corso;

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

EXEC Staging.usp_Reload_Corso;
GO

--DROP TABLE IF EXISTS Fact.Crediti; DROP TABLE IF EXISTS Dim.Corso; DROP SEQUENCE IF EXISTS dbo.seq_Dim_Corso;
GO

IF OBJECT_ID('dbo.seq_Dim_Corso', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_Corso START WITH 1;

END;
GO

IF OBJECT_ID('Dim.Corso', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Corso (
        PKCorso INT NOT NULL CONSTRAINT PK_Dim_Corso PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_Corso_PKCorso DEFAULT (NEXT VALUE FOR dbo.seq_Dim_Corso),
	    IDCorso NVARCHAR(50) NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    Corso NVARCHAR(500) NULL,
	    TipoCorso NVARCHAR(500) NULL,
	    Giornata NVARCHAR(500) NULL,
	    PKDataInizioCorso DATE NOT NULL CONSTRAINT FK_Dim_Corso_PKDataInizioCorso FOREIGN KEY REFERENCES Dim.Data (PKData),
	    OraInizioCorso NVARCHAR(10) NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Corso_id_Corso ON Dim.Corso (IDCorso);
    
    ALTER TABLE Dim.Corso ADD CONSTRAINT DFT_Dim_Corso_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.Corso ADD CONSTRAINT DFT_Dim_Corso_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.Corso ADD CONSTRAINT DFT_Dim_Corso_IsDeleted DEFAULT (0) FOR IsDeleted;

    INSERT INTO Dim.Corso (
        PKCorso,
        IDCorso,
        Corso,
        TipoCorso,
        Giornata,
        PKDataInizioCorso,
        OraInizioCorso
    )
    VALUES
    (   -1, -- PKCorso - int
        N'', -- IDCorso - nvarchar(50)
        N'', -- Corso - nvarchar(500)
        N'', -- TipoCorso - nvarchar(500)
        N'', -- Giornata - nvarchar(500)
        '19000101', -- PKDataInizioCorso - date
        N'' -- OraInizioCorso - nvarchar(500)
    ), (
        -101,
        N'???',
        N'<???>',
        N'',
        N'',
        '19000101',
        N''
    );

    ALTER SEQUENCE dbo.seq_Dim_Corso RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_Corso', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_Corso AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_Corso
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Corso';

    MERGE INTO Dim.Corso AS TGT
    USING Staging.Corso (NOLOCK) AS SRC
    ON SRC.IDCorso = TGT.IDCorso

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IDCorso = SRC.IDCorso,
        TGT.Corso = SRC.Corso,
        TGT.TipoCorso = SRC.TipoCorso,
        TGT.Giornata = SRC.Giornata,
        TGT.PKDataInizioCorso = SRC.PKDataInizioCorso,
        TGT.OraInizioCorso = SRC.OraInizioCorso

    WHEN NOT MATCHED
      THEN INSERT (
        IDCorso,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Corso,
        TipoCorso,
        Giornata,
        PKDataInizioCorso,
        OraInizioCorso
      )
      VALUES (
        SRC.IDCorso,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Corso,
        SRC.TipoCorso,
        SRC.Giornata,
        SRC.PKDataInizioCorso,
        SRC.OraInizioCorso
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Corso' AS full_olap_table_name,
        'IDCorso = ' + CAST(COALESCE(inserted.IDCorso, deleted.IDCorso) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Corso
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Corso;
GO
