USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > GPT
    TABLE_NAME > OpenAIMessage
    STAGING_TABLE_NAME > DomandeMIA
*/

/**
 * @table Staging.DomandeMIA
 * @description

*/

--DROP TABLE IF EXISTS Staging.DomandeMIA; DELETE FROM audit.tables WHERE provider_name = N'GPT' AND full_table_name = N'Landing.GPT_OpenAIMessage';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'GPT' AND full_table_name = N'Landing.GPT_OpenAIMessage')
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
    (   N'GPT',       -- provider_name - nvarchar(60)
        N'Landing.GPT_OpenAIMessage',      -- full_table_name - sysname
        N'Staging.DomandeMIA',      -- staging_table_name - sysname
        N'Fact.DomandeMIA',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

CREATE OR ALTER VIEW Staging.DomandeMIAView
AS
WITH Clienti
AS (
    SELECT
        Id,
        Email

    FROM Landing.GPT_OpenAICliente C
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND (
            C.Email NOT LIKE '%cesimultimedia%'
            AND C.Email NOT LIKE '%partnerup%'
            AND C.Email NOT LIKE '%vincenti%'
            AND C.Email NOT LIKE '%giuggioli%'
        )
),
DomandeMIA
AS (
    SELECT
        M.Id,
        LTRIM (
            RTRIM (
                SUBSTRING (
                    REPLACE (
                        REPLACE (
                            REPLACE (
                                CASE
                                    WHEN CHARINDEX ('§§END-EXTRA-PROMPT-CESI§§', M.Message) > 0 THEN
                                        -- Trova l'ultima posizione invertendo la stringa
                                        SUBSTRING (
                                            M.Message,
                                            LEN (M.Message) - CHARINDEX ('§§ISEC-TPMORP-ARTXE-DNE§§', REVERSE (M.Message))
                                            + 2,
                                            LEN (M.Message)
                                        )
                                    ELSE
                                        M.Message
                                END,
                                CHAR (13),
                                ' '
                            ),
                            CHAR (10),
                            ' '
                        ),
                        CHAR (9),
                        ' '
                    ),
                    0,
                    4000
                )
            )
        ) AS Testo,
        CONVERT(BIT, M.IsQuestion) AS IsDomanda,
        M.CreatedOn AS DataOraCreazione,
        CONVERT(DATE, M.CreatedOn) AS DataCreazione,
        T.Area,
        C.Email

    FROM Landing.GPT_OpenAIMessage M
    INNER JOIN Landing.GPT_OpenAIThread T ON T.Id = M.OpenAIThreadId
        AND T.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Clienti C ON C.Id = T.ClienteId
    WHERE M.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        D.Id,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.Testo,
            D.IsDomanda,
            --D.DataOraCreazione,
            --D.DataCreazione,
            DC.PKData,
            D.Area,
            D.Email,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        D.Testo,
        D.IsDomanda,
        --D.DataOraCreazione,
        --D.DataCreazione,
        DC.PKData AS PKDataCreazione,
        D.Area,
        D.Email

    FROM DomandeMIA D
    INNER JOIN Dim.Data DC ON DC.PKData = D.DataCreazione
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Testo,
    TD.IsDomanda,
    TD.PKDataCreazione,
    TD.Area,
    TD.Email

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.DomandeMIA', N'U') IS NOT NULL DROP TABLE Staging.DomandeMIA;
GO

IF OBJECT_ID(N'Staging.DomandeMIA', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.DomandeMIA FROM Staging.DomandeMIAView;

    ALTER TABLE Staging.DomandeMIA ADD CONSTRAINT PK_Landing_GPT_OpenAIMessage PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    ALTER TABLE Staging.DomandeMIA ALTER COLUMN IsDomanda BIT NOT NULL;
    ALTER TABLE Staging.DomandeMIA ALTER COLUMN PKDataCreazione DATE NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAIMessage_BusinessKey ON Staging.DomandeMIA (Id);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_DomandeMIA', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_DomandeMIA AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_DomandeMIA
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Landing.GPT_OpenAIMessage';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.DomandeMIA;

    INSERT INTO Staging.DomandeMIA
    SELECT * FROM Staging.DomandeMIAView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.DomandeMIA;

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

EXEC Staging.usp_Reload_DomandeMIA;
GO

--DROP TABLE IF EXISTS Fact.DomandeMIA; DROP SEQUENCE IF EXISTS dbo.seq_Fact_DomandeMIA;
GO

IF OBJECT_ID('dbo.seq_Fact_DomandeMIA', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_DomandeMIA START WITH 1;

END;
GO

IF OBJECT_ID('Fact.DomandeMIA', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.DomandeMIA (
        PKDomandeMIA INT NOT NULL CONSTRAINT PK_Fact_DomandeMIA PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_DomandeMIA_PKDomandeMIA DEFAULT (NEXT VALUE FOR dbo.seq_Fact_DomandeMIA),

	    Id INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    PKDataCreazione DATE NOT NULL CONSTRAINT FK_Fact_DomandeMIA_PKDataCreazione FOREIGN KEY REFERENCES Dim.Data (PKData),
	    Testo NVARCHAR(4000) NULL,
	    IsDomanda BIT NOT NULL,
	    Area NVARCHAR(80) NULL,
	    Email NVARCHAR(60) NULL
    );

    ALTER SEQUENCE dbo.seq_Fact_DomandeMIA RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_DomandeMIA', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_DomandeMIA AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_DomandeMIA
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.DomandeMIA AS TGT
    USING Staging.DomandeMIA (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKDataCreazione = SRC.PKDataCreazione,
        TGT.Testo = SRC.Testo,
        TGT.IsDomanda = SRC.IsDomanda,
        TGT.Area = SRC.Area,
        TGT.Email = SRC.Email

    WHEN NOT MATCHED
      THEN INSERT (
        --PKDomandeMIA,
        Id,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataCreazione,
        Testo,
        IsDomanda,
        Area,
        Email
    ) VALUES (
        Id,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataCreazione,
        Testo,
        IsDomanda,
        Area,
        Email
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.DomandeMIA' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.DomandeMIA
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_DomandeMIA;
GO
