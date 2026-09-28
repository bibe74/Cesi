
CREATE VIEW Landing.COMETA_CategoriaCommercialeArticoloView
AS
WITH TableData
AS (
    SELECT
        id_cat_com_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_cat_com_articolo,
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

    FROM COMETA.CategoriaCommercialeArticolo
)
SELECT
    -- Chiavi
    TD.id_cat_com_articolo,

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

