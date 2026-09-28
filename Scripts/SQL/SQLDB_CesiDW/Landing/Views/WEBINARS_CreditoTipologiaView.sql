/**
 * @table Landing.WEBINARS_CreditoTipologia
 * @description 

 * @depends WEBINARS.CreditoTipologia

SELECT TOP (100) * FROM WEBINARS.CreditoTipologia;
*/

CREATE   VIEW Landing.WEBINARS_CreditoTipologiaView
AS
WITH TableData
AS (
    SELECT
        ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Ordine,
            Tipo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Ordine,
        Tipo

    FROM WEBINARS.CreditoTipologia
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Ordine COLLATE DATABASE_DEFAULT AS Ordine,
    TD.Tipo COLLATE DATABASE_DEFAULT AS Tipo

FROM TableData TD;

GO

