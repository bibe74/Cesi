
CREATE VIEW Landing.GPT_OpenAIPartitaView
AS
WITH TableData
AS (
    SELECT
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Codice,
            Descrizione,
            DataCreazione,
            DataScadenza,
            ClienteId,
            Stato,
            Quantita,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        Codice,
        Descrizione,
        DataCreazione,
        DataScadenza,
        ClienteId,
        Stato,
        Quantita

    FROM GPT.OpenAIPartita
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Codice,
    TD.Descrizione,
    TD.DataCreazione,
    TD.DataScadenza,
    TD.ClienteId,
    TD.Stato,

    -- Misure
    TD.Quantita

FROM TableData TD;

GO

