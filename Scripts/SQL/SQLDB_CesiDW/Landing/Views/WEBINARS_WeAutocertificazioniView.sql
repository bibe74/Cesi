/**
 * @table Landing.WEBINARS_WeAutocertificazioni
 * @description 

 * @depends WEBINARS.WeAutocertificazioni

SELECT TOP (100) * FROM WEBINARS.WeAutocertificazioni;
*/

CREATE   VIEW Landing.WEBINARS_WeAutocertificazioniView
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
            Corso,
            Nome,
            Cognome,
            CodiceFiscale,
            Professione,
            Ordine,
            CONVERT(DATE, CreatedOn),
            EMail,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Corso,
        Nome,
        Cognome,
        CodiceFiscale,
        Professione,
        Ordine,
        CONVERT(DATE, CreatedOn) AS DataCreazione,
        EMail

    FROM WEBINARS.WeAutocertificazioni
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
    TD.Corso COLLATE DATABASE_DEFAULT AS Corso,
    TD.Nome COLLATE DATABASE_DEFAULT AS Nome,
    TD.Cognome COLLATE DATABASE_DEFAULT AS Cognome,
    TD.CodiceFiscale COLLATE DATABASE_DEFAULT AS CodiceFiscale,
    TD.Professione COLLATE DATABASE_DEFAULT AS Professione,
    TD.Ordine COLLATE DATABASE_DEFAULT AS Ordine,
    TD.DataCreazione,
    TD.EMail COLLATE DATABASE_DEFAULT AS Email

FROM TableData TD;

GO

