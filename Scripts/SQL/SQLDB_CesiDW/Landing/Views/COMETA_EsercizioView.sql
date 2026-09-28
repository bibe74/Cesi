
CREATE VIEW Landing.COMETA_EsercizioView
AS
WITH TableData
AS (
    SELECT
        id_esercizio,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_esercizio,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            data_inizio,
            data_fine,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        data_inizio,
        data_fine

    FROM COMETA.Esercizio
)
SELECT
    -- Chiavi
    TD.id_esercizio,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.data_inizio,
    TD.data_fine

FROM TableData TD;

GO

