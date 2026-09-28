
CREATE   VIEW Staging.CreditiOpenAIDettaglioView
AS
WITH EmailClienteDettaglio
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
        ECD.PKCliente,
        COALESCE(PA.Codice, '') AS CodiceOrdine,
        COALESCE(DCP.PKData, CAST('19000101' AS DATE)) AS PKDataCreazionePartita,
        COALESCE(DSP.PKData, CAST('19000101' AS DATE)) AS PKDataScadenzaPartita,
        COALESCE(DID.PKData, CAST('19000101' AS DATE)) AS PKDataInizioDemo,
        MAX(COALESCE(DM.PKData, CAST('19000101' AS DATE))) AS PKDataUltimoUtilizzo,
        COALESCE(PA.Quantita, 0) AS QtaCreditiCaricatiInPartita,
        SUM(CR.Quantita) AS QtaCreditiResidui,
        SUM(CASE WHEN CR.Quantita < 0 THEN CR.Quantita * -1 ELSE 0 END) AS QtaCreditiUtilizzati,
        COUNT(1) AS ConteggioDomandeERisposte

    FROM Landing.GPT_OpenAICredito CR
    INNER JOIN Landing.GPT_OpenAICliente CL ON CL.Id = CR.ClienteId
        AND CL.IsDeleted = CAST(0 AS BIT)
    INNER JOIN EmailClienteDettaglio ECD ON ECD.Email = CL.Email
        AND ECD.rn = 1
    INNER JOIN Landing.GPT_OpenAICausale CA ON CA.Id = CR.CausaleId
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.GPT_OpenAIPartita PA ON PA.Id = CR.PartitaId
        AND PA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Data DCP ON DCP.PKData = PA.DataCreazione
    LEFT JOIN Dim.Data DSP ON DSP.PKData = PA.DataScadenza
    LEFT JOIN Landing.MYSOLUTION_Demo D ON CL.Email = D.Email
        AND D.IsDeleted = CAST (0 AS BIT)
    LEFT JOIN Dim.Data DID ON DID.PKData = D.DataInizioDemo
    LEFT JOIN Dim.Data DM ON DM.PKData = CR.DataMovimento
    WHERE CR.IsDeleted = CAST(0 AS BIT)
    GROUP BY ECD.PKCliente,
        COALESCE (PA.Codice, ''),
        COALESCE(DCP.PKData, CAST('19000101' AS DATE)),
        COALESCE(DSP.PKData, CAST('19000101' AS DATE)),
        COALESCE(DID.PKData, CAST('19000101' AS DATE)),
        COALESCE(PA.Quantita, 0)
)
SELECT
    -- Chiavi
    TD.PKCliente,
    TD.CodiceOrdine,
    TD.PKDataUltimoUtilizzo,
    TD.PKDataCreazionePartita,
    TD.PKDataScadenzaPartita,
    TD.QtaCreditiCaricatiInPartita,

    -- Campi per data warehouse
    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        TD.PKCliente,
        TD.CodiceOrdine,
        TD.PKDataUltimoUtilizzo,
        TD.PKDataCreazionePartita,
        TD.PKDataScadenzaPartita,
        TD.QtaCreditiCaricatiInPartita,
        ' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        TD.PKDataInizioDemo,
        TD.QtaCreditiResidui,
        TD.QtaCreditiUtilizzati,
        TD.ConteggioDomandeERisposte,
        ' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,

    -- Attributi
    TD.PKDataInizioDemo,

    -- Misure
    TD.QtaCreditiResidui,
    TD.QtaCreditiUtilizzati,
    TD.ConteggioDomandeERisposte

FROM TableData TD;

GO

