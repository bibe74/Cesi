
CREATE VIEW Landing.COMETA_MovimentiScadenzaView
AS
WITH TableData
AS (
    SELECT
        id_mov_scadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_mov_scadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_scadenza,
            data,
            importo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_scadenza,
        data,
        importo

    FROM COMETA.MovimentiScadenza
)
SELECT
    -- Chiavi
    TD.id_mov_scadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_scadenza,
    TD.data,
    TD.importo

FROM TableData TD;

GO

