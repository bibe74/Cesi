
CREATE VIEW Landing.MYSOLUTION_StateProvinceView
AS
WITH TableData
AS (
    SELECT
        SP.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SP.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SP.CountryId,
            SP.Name,
            SP.Abbreviation,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        SP.CountryId,
        SP.Name,
        SP.Abbreviation

    FROM MYSOLUTION.StateProvince SP
    INNER JOIN MYSOLUTION.Country C ON C.Id = SP.CountryId
        AND C.Published = CAST(1 AS BIT)
    WHERE SP.Published = CAST(1 AS BIT)
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
    TD.CountryId,
    TD.Name,
    TD.Abbreviation

FROM TableData TD;

GO

