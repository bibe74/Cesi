
/**
 * @storedprocedure Fact.usp_ReportCreditiMIA
*/

CREATE   PROCEDURE Fact.usp_ReportCreditiMIA (
    @CapoArea NVARCHAR(60) = NULL,
    @PKDataCreazionePartitaInizio DATE = NULL,
    @PKDataCreazionePartitaFine DATE = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT @PKDataCreazionePartitaInizio = COALESCE(@PKDataCreazionePartitaInizio, DATEADD(DAY, 1, DATEADD(DAY, -DATEPART(DAYOFYEAR, CURRENT_TIMESTAMP), CONVERT(DATE, CURRENT_TIMESTAMP))));
    SELECT @PKDataCreazionePartitaFine = COALESCE(@PKDataCreazionePartitaFine, CONVERT(DATE, CURRENT_TIMESTAMP));

    SELECT
        C.Email,
        COAID.CodiceOrdine AS [Codice ordine],
        COAID.QtaCreditiCaricatiInPartita AS [Crediti caricati in partita],
        COAID.QtaCreditiResidui AS [Crediti residui],
        COAID.QtaCreditiUtilizzati AS [Crediti utilizzati],
        COAID.ConteggioDomandeERisposte AS [Numero domande e risposte],
        DUU.Data_IT AS [Ultimo utilizzo],
        DCP.Data_IT AS [Data creazione partita],
        DSP.Data_IT AS [Data scadenza partita],
        DID.Data_IT AS [Data inizio demo]

    FROM Fact.CreditiOpenAIDettaglio COAID
    INNER JOIN Dim.Cliente C ON C.PKCliente = COAID.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        AND GA.IsDeleted = CAST(0 AS BIT)
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
    INNER JOIN Dim.Data DUU ON DUU.PKData = COAID.PKDataUltimoUtilizzo
    INNER JOIN Dim.Data DCP ON DCP.PKData = COAID.PKDataCreazionePartita
        AND DCP.PKData BETWEEN @PKDataCreazionePartitaInizio AND @PKDataCreazionePartitaFine
    INNER JOIN Dim.Data DSP ON DSP.PKData = COAID.PKDataScadenzaPartita
    INNER JOIN Dim.Data DID ON DID.PKData = COAID.PKDataInizioDemo
    WHERE COAID.IsDeleted = CAST(0 AS BIT)
    ORDER BY C.Email,
        COAID.CodiceOrdine,
        DUU.PKData,
        DCP.PKData,
        DID.PKData;

END;

GO

