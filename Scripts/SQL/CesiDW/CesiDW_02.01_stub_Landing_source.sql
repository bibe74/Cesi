USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > %SCHEMA_NAME%
    TABLE_NAME > %TABLE_NAME%
*/

/**
 * @table Landing.%SCHEMA_NAME%_%TABLE_NAME%
 * @description 

 * @depends %SCHEMA_NAME%.%TABLE_NAME%

SELECT TOP 100 * FROM %SCHEMA_NAME%.%TABLE_NAME%;
*/

IF OBJECT_ID('Landing.%SCHEMA_NAME%_%TABLE_NAME%View', 'V') IS NULL EXEC('CREATE VIEW Landing.%SCHEMA_NAME%_%TABLE_NAME%View AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.%SCHEMA_NAME%_%TABLE_NAME%View
AS
WITH TableData
AS (
    SELECT

        --CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        --    ' '
        --))) AS HistoricalHashKey,
        --CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        --    ' '
        --))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        *

    FROM %SCHEMA_NAME%.%TABLE_NAME%
)
SELECT
    -- Chiavi

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.*

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.%SCHEMA_NAME%_%TABLE_NAME%;
GO

IF OBJECT_ID(N'Landing.%SCHEMA_NAME%_%TABLE_NAME%', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.%SCHEMA_NAME%_%TABLE_NAME% FROM Landing.%SCHEMA_NAME%_%TABLE_NAME%View;

    ALTER TABLE Landing.%SCHEMA_NAME%_%TABLE_NAME% ADD CONSTRAINT PK_Landing_%SCHEMA_NAME%_%TABLE_NAME% PRIMARY KEY CLUSTERED (UpdateDatetime, id_%TABLE_NAME%);

    ALTER TABLE Landing.%SCHEMA_NAME%_%TABLE_NAME% ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_%SCHEMA_NAME%_%TABLE_NAME%_BusinessKey ON Landing.%SCHEMA_NAME%_%TABLE_NAME% (id_%TABLE_NAME%);
END;
GO

IF OBJECT_ID('%SCHEMA_NAME%.usp_Merge_%TABLE_NAME%', 'P') IS NULL EXEC('CREATE PROCEDURE %SCHEMA_NAME%.usp_Merge_%TABLE_NAME% AS RETURN 0;');
GO

ALTER PROCEDURE %SCHEMA_NAME%.usp_Merge_%TABLE_NAME%
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.%SCHEMA_NAME%_%TABLE_NAME% AS TGT
    USING Landing.%SCHEMA_NAME%_%TABLE_NAME%View (nolock) AS SRC
    ON SRC.id_%TABLE_NAME% = TGT.id_%TABLE_NAME%

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.* = SRC.*

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_%TABLE_NAME%,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        *
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.%SCHEMA_NAME%_%TABLE_NAME%' AS full_olap_table_name,
        'id_%TABLE_NAME% = ' + CAST(COALESCE(inserted.id_%TABLE_NAME%, deleted.id_%TABLE_NAME%) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC %SCHEMA_NAME%.usp_Merge_%TABLE_NAME%;
GO
