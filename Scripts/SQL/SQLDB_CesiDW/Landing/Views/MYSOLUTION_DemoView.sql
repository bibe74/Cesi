/**
 * @table Landing.MYSOLUTION_Demo
 * @description

*/

CREATE   VIEW Landing.MYSOLUTION_DemoView
AS
WITH TableData
AS (
    SELECT DISTINCT
        T.ID AS Id,
    
        CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
    	    T.ID,
    	    ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
            T.Email,
            T.RagioneSociale,
            T.Nome,
            T.Cognome,
            T.Citta,
            T.Provincia,
            T.ProvinciaAbbr,
            CONVERT(DATE, T.CreatedOnUtc),
    	    ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
    
        T.EMAIL AS Email,
        T.RagioneSociale,
        T.Nome,
        T.Cognome,
        T.Citta,
        T.Provincia,
        T.ProvinciaAbbr AS SiglaProvincia,
        CONVERT(DATE, T.CreatedOnUtc) AS DataInizioDemo
    
    FROM MYSOLUTION.Demo T
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Email,
    TD.RagioneSociale,
    TD.Nome,
    TD.Cognome,
    TD.Citta,
    TD.Provincia,
    TD.SiglaProvincia,
    TD.DataInizioDemo

FROM TableData TD;

GO

