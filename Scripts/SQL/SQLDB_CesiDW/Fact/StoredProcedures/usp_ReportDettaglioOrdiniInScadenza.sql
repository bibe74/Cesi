
/**
 * @storedprocedure Fact.usp_ReportDettaglioOrdiniInScadenza
*/

CREATE   PROCEDURE Fact.usp_ReportDettaglioOrdiniInScadenza (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60),
    --@TipoFiltroData CHAR(1) = 'M', -- 'M': mese, 'P': periodo
    @TipoData CHAR(1) = 'F', -- 'C': competenza (data ordine), 'I': inizio contratto, 'F': fine contratto
    @RagioneSociale NVARCHAR(120) = NULL,
    @CodiceCliente NVARCHAR(10) = NULL,
    @PartitaIVA NVARCHAR(20) = NULL,
    @Azione NVARCHAR(60) = NULL
)
AS
BEGIN

SET NOCOUNT ON;

IF (@PKDataInizioPeriodo IS NULL)
BEGIN
    SELECT @PKDataInizioPeriodo = DATEADD(DAY, 1-DATEPART(DAY, CURRENT_TIMESTAMP), CAST(CURRENT_TIMESTAMP AS DATE));
END;

IF (@PKDataFinePeriodo IS NULL)
BEGIN
    SELECT @PKDataFinePeriodo = DATEADD(DAY, -1, DATEADD(MONTH, 3, @PKDataInizioPeriodo));
END;

EXEC Fact.usp_ReportDettaglioOrdini
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @PKDataFinePeriodo = @PKDataFinePeriodo,
    @GruppoAgenti = @GruppoAgenti,
    @CapoArea = @CapoArea,
    @TipoData = @TipoData,
    @RagioneSociale = @RagioneSociale,
    @CodiceCliente = @CodiceCliente,
    @PartitaIVA = @PartitaIVA,
    @Azione = @Azione,
    @NascondiOrdiniRinnovati = 1;

END;

GO

