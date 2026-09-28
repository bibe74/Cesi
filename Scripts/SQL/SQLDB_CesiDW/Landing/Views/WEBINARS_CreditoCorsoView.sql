/**
 * @table Landing.WEBINARS_CreditoCorso
 * @description 

 * @depends WEBINARS.CreditoCorso

SELECT TOP (100) * FROM WEBINARS.CreditoCorso;
*/

CREATE   VIEW Landing.WEBINARS_CreditoCorsoView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CreditoTipologiaID,
            WebinarSource,
            Autocertificazione,
            Crediti,
            Ora,
            CodiceMateria,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        CreditoTipologiaID,
        WebinarSource,
        Autocertificazione,
        Crediti,
        Ora,
        CodiceMateria

    FROM WEBINARS.CreditoCorso
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
    TD.CreditoTipologiaID,
    TD.WebinarSource COLLATE DATABASE_DEFAULT AS WebinarSource,
    TD.Autocertificazione,
    TD.Crediti,
    TD.Ora,
    TD.CodiceMateria COLLATE DATABASE_DEFAULT AS CodiceMateria

FROM TableData TD;

GO

