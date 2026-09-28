
CREATE VIEW Landing.COMETA_RegistroView
AS
WITH TableData
AS (
    SELECT
        id_registro,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_registro,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_esercizio,
            tipo_registro,
            id_mod_registro,
            numero,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_esercizio,
        tipo_registro,
        id_mod_registro,
        numero,
        descrizione

    FROM COMETA.Registro
)
SELECT
    -- Chiavi
    TD.id_registro,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_esercizio,
    TD.tipo_registro COLLATE DATABASE_DEFAULT AS tipo_registro,
    TD.id_mod_registro,
    TD.numero,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione

FROM TableData TD;

GO

