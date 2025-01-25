USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETAINTEGRATION
    TABLE_NAME > ArticleBIData
*/

/**
 * @table Landing.COMETAINTEGRATION_ArticleBIData
 * @description 

 * @depends COMETAINTEGRATION.ArticleBIData

SELECT TOP (100) * FROM COMETAINTEGRATION.ArticleBIData;
*/

CREATE OR ALTER VIEW Landing.COMETAINTEGRATION_ArticleBIDataView
AS
WITH TableData
AS (
    SELECT
        ArticleID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ArticleID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Data1,
            Data2,
            Data3,
            Data4,
            Data5,
            Data6,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6

    FROM COMETAINTEGRATION.ArticleBIData
)
SELECT
    -- Chiavi
    TD.ArticleID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Data1 COLLATE DATABASE_DEFAULT AS Data1,
    TD.Data2 COLLATE DATABASE_DEFAULT AS Data2,
    TD.Data3 COLLATE DATABASE_DEFAULT AS Data3,
    TD.Data4 COLLATE DATABASE_DEFAULT AS Data4,
    TD.Data5 COLLATE DATABASE_DEFAULT AS Data5,
    TD.Data6 COLLATE DATABASE_DEFAULT AS Data6

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETAINTEGRATION_ArticleBIData;
GO

IF OBJECT_ID(N'Landing.COMETAINTEGRATION_ArticleBIData', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETAINTEGRATION_ArticleBIData FROM Landing.COMETAINTEGRATION_ArticleBIDataView;

    ALTER TABLE Landing.COMETAINTEGRATION_ArticleBIData ALTER COLUMN ArticleID INT NOT NULL;

    ALTER TABLE Landing.COMETAINTEGRATION_ArticleBIData ADD CONSTRAINT PK_Landing_COMETAINTEGRATION_ArticleBIData PRIMARY KEY CLUSTERED (UpdateDatetime, ArticleID);

    --ALTER TABLE Landing.COMETAINTEGRATION_ArticleBIData ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETAINTEGRATION_ArticleBIData_BusinessKey ON Landing.COMETAINTEGRATION_ArticleBIData (ArticleID);
END;
GO

CREATE OR ALTER PROCEDURE COMETAINTEGRATION.usp_Merge_ArticleBIData
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETAINTEGRATION_ArticleBIData AS TGT
    USING Landing.COMETAINTEGRATION_ArticleBIDataView (nolock) AS SRC
    ON SRC.ArticleID = TGT.ArticleID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.Data1 = SRC.Data1,
        TGT.Data2 = SRC.Data2,
        TGT.Data3 = SRC.Data3,
        TGT.Data4 = SRC.Data4,
        TGT.Data5 = SRC.Data5,
        TGT.Data6 = SRC.Data6

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ArticleID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6
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
        'Landing.COMETAINTEGRATION_ArticleBIData' AS full_olap_table_name,
        'ArticleID = ' + CAST(COALESCE(inserted.ArticleID, deleted.ArticleID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETAINTEGRATION.usp_Merge_ArticleBIData;
GO
