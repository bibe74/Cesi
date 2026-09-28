
CREATE VIEW Landing.COMETA_Profilo_DocumentoView
AS
WITH TableData
AS (
    SELECT
        id_prof_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_prof_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            tipo_registro,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        tipo_registro

    FROM COMETA.Profilo_Documento
)
SELECT
    -- Chiavi
    TD.id_prof_documento,

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
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.tipo_registro COLLATE DATABASE_DEFAULT AS tipo_registro

FROM TableData TD;

GO

