
CREATE VIEW Landing.COMETA_Tipo_FatturazioneView
AS
WITH TableData
AS (
    SELECT
        id_tipo_fatturazione,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_tipo_fatturazione,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.Tipo_Fatturazione
)
SELECT
    -- Chiavi
    TD.id_tipo_fatturazione,

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
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione

FROM TableData TD;

GO

