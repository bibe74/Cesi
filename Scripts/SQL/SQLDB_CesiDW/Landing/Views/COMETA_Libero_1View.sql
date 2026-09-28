
CREATE VIEW Landing.COMETA_Libero_1View
AS
WITH TableData
AS (
    SELECT
        id_libero_1,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_libero_1,
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

    FROM COMETA.Libero_1
)
SELECT
    -- Chiavi
    TD.id_libero_1,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;

GO

