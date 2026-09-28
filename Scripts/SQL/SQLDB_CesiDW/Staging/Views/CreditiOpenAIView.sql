CREATE   VIEW Staging.CreditiOpenAIView
AS
WITH OpenAICrediti
AS (
    SELECT
        C.Email,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'C' THEN CR.Quantita ELSE 0 END) AS CreditiAcquistati,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'S' AND CR.PartitaId IS NOT NULL THEN CR.Quantita ELSE 0 END) AS CreditiConsumati,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'S' AND CR.PartitaId IS NULL THEN CR.Quantita ELSE 0 END) AS CreditiFuoriOrdine,
        SUM(CR.Quantita) AS CreditiResidui,
        MIN(P.datascadenza) AS DataScadenzaOrdine

    FROM Landing.GPT_OpenAICredito CR
    INNER JOIN Landing.GPT_OpenAICliente C ON C.Id = CR.ClienteId
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.GPT_OpenAICausale CA ON CA.Id = CR.CausaleId
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.GPT_OpenAIPartita P ON P.Id = CR.PartitaId
    WHERE CR.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.Email
),
EmailClienteDettaglio
AS (
    SELECT
        C.Email,
        C.PKCliente,
        ROW_NUMBER() OVER (PARTITION BY C.Email ORDER BY C.PKCliente DESC) AS rn

    FROM Dim.Cliente C
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND C.Email LIKE N'%@%'

),
TableData
AS (
    SELECT
        --OAIC.Email,
        ECD.PKCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ECD.PKCliente,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DSO.PKData,
            OAIC.CreditiAcquistati,
            OAIC.CreditiConsumati,
            OAIC.CreditiFuoriOrdine,
            OAIC.CreditiResidui,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        --OAIC.scadenza_ordine,
        DSO.PKData AS PKDataScadenzaOrdine,
        OAIC.CreditiAcquistati,
        OAIC.CreditiConsumati,
        OAIC.CreditiFuoriOrdine,
        OAIC.CreditiResidui

    FROM OpenAICrediti OAIC
    INNER JOIN Dim.Data DSO ON DSO.PKData = OAIC.DataScadenzaOrdine
    INNER JOIN EmailClienteDettaglio ECD ON ECD.Email = OAIC.Email
        AND ECD.rn = 1
)
SELECT
    -- Chiavi
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.PKDataScadenzaOrdine,

    -- Misure
    TD.CreditiAcquistati,
    TD.CreditiConsumati,
    TD.CreditiFuoriOrdine,
    TD.CreditiResidui

FROM TableData TD;

GO

