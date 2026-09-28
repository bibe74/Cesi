
CREATE VIEW Landing.COMETA_CategoriaCommercialeSoggettoCommercialeView
AS
WITH TableData
AS (
    SELECT
        id_cat_com_sc,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_cat_com_sc,
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

    FROM COMETA.CategoriaCommercialeSoggettoCommerciale
)
SELECT
    -- Chiavi
    TD.id_cat_com_sc,

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
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione

FROM TableData TD;

GO

