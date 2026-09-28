
CREATE VIEW Landing.COMETA_ScadenzaView
AS
WITH TableData
AS (
    SELECT
        id_scadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_scadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            tipo_scadenza,
            id_sog_commerciale,
            data_scadenza,
            importo,
            stato_scadenza,
            esito_pagamento,
            id_documento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        tipo_scadenza,
        id_sog_commerciale,
        data_scadenza,
        importo,
        stato_scadenza,
        esito_pagamento,
        id_documento

    FROM COMETA.Scadenza
)
SELECT
    -- Chiavi
    TD.id_scadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.tipo_scadenza,
    TD.id_sog_commerciale,
    TD.data_scadenza,
    TD.importo,
    TD.stato_scadenza,
    TD.esito_pagamento,
    TD.id_documento

FROM TableData TD;

GO

