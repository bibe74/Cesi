
CREATE VIEW Landing.COMETA_CondizioniPagamentoView
AS
WITH TableData
AS (
    SELECT
        id_con_pagamento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_con_pagamento,
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

    FROM COMETA.CondizioniPagamento
)
SELECT
    -- Chiavi
    TD.id_con_pagamento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS Codice,
    TD.descrizione COLLATE DATABASE_DEFAULT AS Descrizione

FROM TableData TD;

GO

