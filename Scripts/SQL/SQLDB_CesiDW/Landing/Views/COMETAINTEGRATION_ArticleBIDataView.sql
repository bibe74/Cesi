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

CREATE   VIEW Landing.COMETAINTEGRATION_ArticleBIDataView
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

