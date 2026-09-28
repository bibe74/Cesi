
CREATE VIEW Staging.CapoAreaView
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

