
CREATE VIEW Landing.COMETA_TelefonoView
AS
WITH TableData
AS (
    SELECT
        id_telefono,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_telefono,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            tipo,
            num_riferimento,
            descrizione,
            interlocutore,
            nome,
            cognome,
            ruolo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_anagrafica,
        tipo,
        num_riferimento,
        descrizione,
        interlocutore,
        nome,
        cognome,
        ruolo

    FROM COMETA.Telefono
    WHERE num_riferimento IS NOT NULL
)
SELECT
    -- Chiavi
    TD.id_telefono,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_anagrafica,
    TD.tipo,
    LOWER(TD.num_riferimento) COLLATE DATABASE_DEFAULT AS num_riferimento,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.interlocutore COLLATE DATABASE_DEFAULT AS interlocutore,
    TD.nome COLLATE DATABASE_DEFAULT AS nome,
    TD.cognome COLLATE DATABASE_DEFAULT AS cognome,
    TD.ruolo

FROM TableData TD;

GO

