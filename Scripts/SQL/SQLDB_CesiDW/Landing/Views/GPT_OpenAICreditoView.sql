
CREATE VIEW Landing.GPT_OpenAICreditoView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ClienteId,
            CausaleId,
            PartitaId,
            MessageId,
            Documento,
            DataMovimento,
            Quantita,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        ClienteId,
        CausaleId,
        PartitaId,
        MessageId,
        Documento,
        DataMovimento,
        Quantita

    FROM GPT.OpenAICredito
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
    TD.ClienteId,
    TD.CausaleId,
    TD.PartitaId,
    TD.MessageId,
    TD.Documento,
    TD.DataMovimento,

    -- Misure
    TD.Quantita

FROM TableData TD;

GO

