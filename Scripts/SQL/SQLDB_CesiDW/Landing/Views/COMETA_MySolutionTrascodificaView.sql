
CREATE VIEW Landing.COMETA_MySolutionTrascodificaView
AS
WITH TableData
AS (
    SELECT
        codice,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            tipo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        tipo

    FROM COMETA.MySolutionTrascodifica
)
SELECT
    -- Chiavi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.tipo COLLATE DATABASE_DEFAULT AS tipo

FROM TableData TD;

GO

