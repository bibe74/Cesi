/**
 * @storedprocedure Fact.usp_ReportUtilizzoOpenAI
*/

CREATE   PROCEDURE Fact.usp_ReportUtilizzoOpenAI (
    @CapoArea NVARCHAR(60) = NULL,
    @PKDataConversazioneInizio DATE = NULL,
    @PKDataConversazioneFine DATE = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT @PKDataConversazioneInizio = COALESCE(@PKDataConversazioneInizio, DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP)));
    SELECT @PKDataConversazioneFine = COALESCE(@PKDataConversazioneFine, DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP)));

    SELECT
        C.Email,
        C.CodiceCliente,
        C.RagioneSociale,
        UOAI.IDThread,
        UOAI.IDConversazione,
        UOAI.PKDataConversazione,
        DC.Data_IT AS [Data Conversazione],
        UOAI.Area,
        UOAI.ModalitaRisposta,
        UOAI.Modello,
        UOAI.IDVectorStorage,
        UOAI.InputTokens,
        UOAI.OutputTokens,
        UOAI.ReasoningTokens,
        UOAI.TotalTokens,
        UOAI.EstimatedTotalCostUSD

    FROM Fact.UtilizzoOpenAI UOAI
    INNER JOIN Dim.Cliente C ON C.PKCliente = UOAI.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        AND GA.IsDeleted = CAST(0 AS BIT)
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
    INNER JOIN Dim.Data DC ON DC.PKData = UOAI.PKDataConversazione
        AND DC.PKData BETWEEN @PKDataConversazioneInizio AND @PKDataConversazioneFine
    WHERE UOAI.IsDeleted = CAST(0 AS BIT)
    ORDER BY C.Email,
        UOAI.PKDataConversazione,
        UOAI.PKUtilizzoOpenAI;

END;

GO

