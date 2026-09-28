/**
 * @table Landing.WEBINARS_CreditoAutocertificazione
 * @description 

 * @depends WEBINARS.CreditoAutocertificazione

SELECT TOP (100) * FROM WEBINARS.CreditoAutocertificazione;
*/

CREATE   VIEW Landing.WEBINARS_CreditoAutocertificazioneView
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
            AutocertificazioneID,
            CreditoTipologiaID,
            CreditoCorsoID,
            Crediti,
            Stato,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        AutocertificazioneID,
        CreditoTipologiaID,
        CreditoCorsoID,
        Crediti,
        Stato

    FROM WEBINARS.CreditoAutocertificazione
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
    TD.AutocertificazioneID,
    TD.CreditoTipologiaID,
    TD.CreditoCorsoID,
    TD.Stato,

    -- Misure
    TD.Crediti

FROM TableData TD;

GO

