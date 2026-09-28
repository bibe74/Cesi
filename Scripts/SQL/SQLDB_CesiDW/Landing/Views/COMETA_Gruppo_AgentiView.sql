
CREATE VIEW Landing.COMETA_Gruppo_AgentiView
AS
WITH TableData
AS (
    SELECT
        id_gruppo_agenti,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_gruppo_agenti,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            id_sog_com_capo_area,
            id_sog_com_agente,
            id_sog_com_sub_agente,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        id_sog_com_capo_area,
        id_sog_com_agente,
        id_sog_com_sub_agente

    FROM COMETA.Gruppo_Agenti
)
SELECT
    -- Chiavi
    TD.id_gruppo_agenti,

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
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.id_sog_com_capo_area,
    TD.id_sog_com_agente,
    TD.id_sog_com_sub_agente

FROM TableData TD;

GO

