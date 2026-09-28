
CREATE VIEW Landing.COMETA_ArticoloView
AS
WITH TableData
AS (
    SELECT
        id_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_articolo,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            des_breve,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        id_cat_com_articolo,
        id_cat_merceologica,
        des_breve

    FROM COMETA.Articolo
)
SELECT
    -- Chiavi
    TD.id_articolo,

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
    TD.descrizione,
    TD.id_cat_com_articolo,
    TD.id_cat_merceologica,
    TD.des_breve

FROM TableData TD;

GO

