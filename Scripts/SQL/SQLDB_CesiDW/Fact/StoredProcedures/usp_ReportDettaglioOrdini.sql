
/**
 * @storedprocedure Fact.usp_ReportDettaglioOrdini
*/

CREATE   PROCEDURE Fact.usp_ReportDettaglioOrdini (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60),
    --@TipoFiltroData CHAR(1) = 'M', -- 'M': mese, 'P': periodo
    @TipoData CHAR(1) = 'C', -- 'C': competenza (data ordine), 'I': inizio contratto, 'F': fine contratto
    @RagioneSociale NVARCHAR(120) = NULL,
    @CodiceCliente NVARCHAR(10) = NULL,
    @PartitaIVA NVARCHAR(20) = NULL,
    @Azione NVARCHAR(60) = NULL,
    @NascondiOrdiniRinnovati BIT = 0,
    @HasAbbonamentoMySolution BIT = NULL,
    @HasAbbonamentoMIA BIT = NULL
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
    SELECT @PKDataFinePeriodo = DATEADD(MONTH, 1, @PKDataInizioPeriodo);
END;

DECLARE @AgenteProprietarioPrefix NVARCHAR(20) = N'Proprietario(';

DROP TABLE IF EXISTS #ReportData;

WITH Insoluti
AS (
    SELECT
        D.PKCliente,
        SUM(S.ImportoResiduo) AS Insoluto
    FROM Fact.Scadenze S
    INNER JOIN Fact.Documenti D ON D.PKDocumenti = S.PKDocumenti
        AND D.IsDeleted = CAST(0 AS BIT)
    WHERE S.IsDeleted = CAST(0 AS BIT)
    GROUP BY D.PKCliente
    HAVING SUM(S.ImportoResiduo) > 0.0
),
RigaOrdineFattura
AS (
    SELECT
        O.IDDocumento_Riga,
        MAX(F.PKDataCompetenza) AS PKDataFattura
    FROM Fact.Documenti O
    INNER JOIN Fact.Documenti F ON F.IDDocumento_Riga_Provenienza = O.IDDocumento_Riga
        AND F.IsDeleted = CAST(0 AS BIT)
    WHERE O.Profilo = N'ORDINE CLIENTE'
        AND O.IsDeleted = CAST(0 AS BIT)
    GROUP BY O.IDDocumento_Riga
),
Ordini
AS (
    SELECT
        D.IDDocumento,
        C.CodiceCliente,
        C.RagioneSociale,
        C.Indirizzo,
        C.Localita AS Citta,
        C.Provincia,
        C.PartitaIVA,
        GA.GruppoAgenti AS Agente,
        GA.CapoArea AS AgenteAssegnato,
        CASE WHEN D.NoteDecisionali LIKE @AgenteProprietarioPrefix + N'%)' THEN SUBSTRING(D.NoteDecisionali, LEN(@AgenteProprietarioPrefix)+1, LEN(D.NoteDecisionali) - LEN(@AgenteProprietarioPrefix) - 1) ELSE GA.CapoArea END AS AgenteProprietario,
        D.Libero1 AS Azione,
        D.RinnovoAutomatico AS Rinnovo,
        D.PKDataInizioContratto,
        DIC.Data_IT AS DataInizioContratto,
        D.PKDataFineContratto,
        DFC.Data_IT AS DataFineContratto,
        D.PKDataCompetenza AS PKDataDocumento,
        DC.Data_IT AS DataDocumento,
        D.NumeroDocumento,
        D.CodiceCondizioniPagamento AS CodicePagamento,
        D.CondizioniPagamento AS Pagamento,
        D.Libero2 AS Progetto,
        --A.Codice AS CodiceArticolo,
        --A.Descrizione AS TipoAbbonamento,
        SUM(D.ImportoTotale) AS TotaleDocumento,
        COALESCE(I.Insoluto, 0.0) AS Insoluto,
        C.PKDataDisdetta,
        DDIS.Data_IT AS DataDisdetta,
        C.MotivoDisdetta,
        --D.NumeroRiga,
        COALESCE(ICA.Prefisso, N'XXX') AS PrefissoCapoArea,
        SUM(D.ImportoProvvigioneCapoArea) AS ImportoProvvigioneCapoArea,
        SUM(D.ImportoProvvigioneAgente) AS ImportoProvvigioneAgente,
        SUM(D.ImportoProvvigioneSubagente) AS ImportoProvvigioneSubagente,
        A.Fatturazione,
        D.Progressivo,
		SUM(CASE WHEN D.NumeroRiga = 1 THEN D.Quote ELSE NULL END) AS Quote,

        --C.TipoCliente,
        A.Tipo AS TipoCliente,

        D.TipoFatturazione,
        COALESCE(MAX(ROF.PKDataFattura), CAST('19000101' AS DATE)) AS PKDataFattura,
        D.NoteIntestazione,
        C.Email,
        COALESCE(CASE MT.MacroTipologia
          WHEN N'Nuova vendita' THEN ICA.ProvvigioneNuovo
          WHEN N'Rinnovo' THEN ICA.ProvvigioneRinnovo
          WHEN N'Rinnovo automatico' THEN 10.0
          ELSE NULL
        END, 0.0) / 100.0 AS ProvvigioneTeorica,
        COALESCE(CASE WHEN DATEDIFF(MONTH, D.PKDataInizioContratto, D.PKDataFineContratto) >= 24 THEN LPTP.LiquidazioneProvvigioneTeorica ELSE LPTA.LiquidazioneProvvigioneTeorica END, N'') AS LiquidazioneProvvigioneTeorica,
        D.IDDocumentoRinnovato,
        D.PKDataCompetenza,
        A.LivelloMIA

    FROM Fact.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
        AND (
            @RagioneSociale IS NULL
            OR C.RagioneSociale LIKE N'%' + @RagioneSociale + N'%'
        )
        AND (
            @CodiceCliente IS NULL
            OR C.CodiceCliente = @CodiceCliente
        )
        AND (
            @PartitaIVA IS NULL
            OR C.PartitaIVA = @PartitaIVA
        )
        AND (
            @HasAbbonamentoMySolution IS NULL
            OR C.HasAbbonamentoMySolution = @HasAbbonamentoMySolution
        )
        AND (
            @HasAbbonamentoMIA IS NULL
            OR C.HasAbbonamentoMIA = @HasAbbonamentoMIA
        )
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
        AND (
            @GruppoAgenti IS NULL
            OR GA.GruppoAgenti = @GruppoAgenti
        )
    LEFT JOIN IMPORT.CapiArea ICA ON ICA.CapoArea = GA.CapoArea
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
    INNER JOIN Dim.Data DIC ON DIC.PKData = D.PKDataInizioContratto
        --AND (
        --    @TipoData <> 'I'
        --    OR DIC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        --)
    INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
        --AND (
        --    @TipoData <> 'F'
        --    OR DFC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        --)
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        --AND (
        --    @TipoData <> 'C'
        --    OR DC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        --)
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
    INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia
    LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
    LEFT JOIN RigaOrdineFattura ROF ON ROF.IDDocumento_Riga = D.IDDocumento_Riga
    LEFT JOIN Import.LiquidazioneProvvigioneTeorica LPTA ON LPTA.CodiceCondizioniPagamento = D.CodiceCondizioniPagamento
        AND LPTA.DurataContratto = N'Annuale'
    LEFT JOIN Import.LiquidazioneProvvigioneTeorica LPTP ON LPTP.CodiceCondizioniPagamento = D.CodiceCondizioniPagamento
        AND LPTP.DurataContratto = N'Pluriennale'
    WHERE D.Profilo = N'ORDINE CLIENTE'
        AND D.IsDeleted = CAST(0 AS BIT)
        AND (
            @Azione IS NULL
            OR D.Libero1 = @Azione
        )
    GROUP BY COALESCE (I.Insoluto, 0.0),
        COALESCE (ICA.Prefisso, N'XXX'),
        D.IDDocumento,
        C.CodiceCliente,
        C.RagioneSociale,
        C.Indirizzo,
        C.Localita,
        C.Provincia,
        C.PartitaIVA,
        GA.GruppoAgenti,
        GA.CapoArea,
        D.Libero1,
        D.RinnovoAutomatico,
        D.PKDataInizioContratto,
        DIC.Data_IT,
        D.PKDataFineContratto,
        DFC.Data_IT,
        D.PKDataCompetenza,
        DC.Data_IT,
        D.NumeroDocumento,
        D.CodiceCondizioniPagamento,
        D.CondizioniPagamento,
        D.Libero2,
        C.PKDataDisdetta,
        DDIS.Data_IT,
        C.MotivoDisdetta,
        A.Fatturazione,
        D.Progressivo,
        --C.TipoCliente,
        A.Tipo,
        D.TipoFatturazione,
        D.NoteIntestazione,
        C.Email,
        MT.MacroTipologia,
        ICA.ProvvigioneNuovo,
        ICA.ProvvigioneRinnovo,
        CASE WHEN D.NoteDecisionali LIKE @AgenteProprietarioPrefix + N'%)' THEN SUBSTRING(D.NoteDecisionali, LEN(@AgenteProprietarioPrefix)+1, LEN(D.NoteDecisionali) - LEN(@AgenteProprietarioPrefix) - 1) ELSE GA.CapoArea END,
        COALESCE(CASE WHEN DATEDIFF(MONTH, D.PKDataInizioContratto, D.PKDataFineContratto) >= 24 THEN LPTP.LiquidazioneProvvigioneTeorica ELSE LPTA.LiquidazioneProvvigioneTeorica END, N''),
        D.IDDocumentoRinnovato,
        D.PKDataCompetenza,
        A.LivelloMIA
),
DettaglioOrdini
AS (
    SELECT
        O.IDDocumento,
        O.CodiceCliente,
        O.RagioneSociale,
        O.Indirizzo,
        O.Citta,
        O.Provincia,
        O.PartitaIVA,
        O.Agente,
        O.AgenteAssegnato,
        O.Azione,
        O.Rinnovo,
        O.PKDataInizioContratto,
        O.DataInizioContratto,
        O.PKDataFineContratto,
        O.DataFineContratto,
        O.PKDataDocumento,
        O.DataDocumento,
        O.NumeroDocumento,
        O.CodicePagamento,
        O.Pagamento,
        O.Progetto,
        O.TotaleDocumento,
        O.Insoluto,
        O.PKDataDisdetta,
        O.DataDisdetta,
        O.MotivoDisdetta,
        O.PrefissoCapoArea,
        O.ImportoProvvigioneCapoArea,
        O.ImportoProvvigioneAgente,
        O.ImportoProvvigioneSubagente,
        O.Fatturazione,
        O.Progressivo,
		O.Quote,
        ROW_NUMBER() OVER (PARTITION BY O.CodiceCliente ORDER BY O.NumeroDocumento) AS rn,
        DENSE_RANK() OVER (PARTITION BY O.PrefissoCapoArea ORDER BY O.CodiceCliente, O.NumeroDocumento) AS rnCapoArea,
        O.TipoCliente,
        O.TipoFatturazione,
        O.PKDataFattura,
        O.NoteIntestazione,
        O.Email,
        O.ProvvigioneTeorica,
        O.LiquidazioneProvvigioneTeorica,
        O.AgenteProprietario,
        O.IDDocumentoRinnovato,
        O.PKDataCompetenza,
        O.LivelloMIA

    FROM Ordini O
)
SELECT
    DO.CodiceCliente,
    DO.RagioneSociale,
    DO.Indirizzo,
    DO.Citta,
    DO.Provincia,
    DO.PartitaIVA,
    DO.Agente,
    DO.AgenteAssegnato,
    DO.Azione,
    DO.Rinnovo,
    DO.PKDataInizioContratto,
    DO.DataInizioContratto,
    DO.PKDataFineContratto,
    DO.DataFineContratto,
    DO.PKDataDocumento,
    DO.DataDocumento,
    DO.NumeroDocumento,
    DO.CodicePagamento,
    DO.Pagamento,
    DO.Progetto,
    --DO.CodiceArticolo,
    --DO.TipoAbbonamento,
    DO.TotaleDocumento,
    CASE WHEN DO.rn = 1 THEN DO.Insoluto ELSE 0.0 END AS Insoluto,
    DO.PKDataDisdetta,
    DO.DataDisdetta,
    DO.MotivoDisdetta,
    DO.PrefissoCapoArea + RIGHT(N'0000' + CONVERT(NVARCHAR(4), DO.rnCapoArea), 4) AS ProgressivoAgenteAssegnato,
    DO.ImportoProvvigioneCapoArea,
    DO.ImportoProvvigioneAgente,
    DO.ImportoProvvigioneSubagente,
    DATEDIFF(MONTH, DO.PKDataInizioContratto, DATEADD(DAY, 1, DO.PKDataFineContratto)) AS DurataMesi,
    DO.Fatturazione,
    DO.Progressivo,
	DO.Quote,
    DO.TipoCliente,
    DO.TipoFatturazione,
    DO.PKDataFattura,
    DO.NoteIntestazione,
    DO.Email,
    DO.ProvvigioneTeorica,
    CONVERT(NVARCHAR(120), DO.LiquidazioneProvvigioneTeorica) AS LiquidazioneProvvigioneTeorica,
    DO.AgenteProprietario,

    DOR.TotaleDocumento AS TotaleRinnovo,
    DOR.DataDocumento AS DataRinnovo,
    DOR.TipoFatturazione AS TipoFatturazioneRinnovo,
    DATEDIFF(MONTH, DOR.PKDataInizioContratto, DATEADD(DAY, 1, DOR.PKDataFineContratto)) AS DurataMesiRinnovo,
    DO.LivelloMIA

INTO #ReportData

FROM DettaglioOrdini DO
LEFT JOIN DettaglioOrdini DOR ON DOR.IDDocumentoRinnovato = DO.IDDocumento
INNER JOIN Dim.Data DIC ON DIC.PKData = DO.PKDataInizioContratto
    AND (
        @TipoData <> 'I'
        OR DIC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
    )
INNER JOIN Dim.Data DFC ON DFC.PKData = DO.PKDataFineContratto
    AND (
        @TipoData <> 'F'
        OR DFC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
    )
INNER JOIN Dim.Data DC ON DC.PKData = DO.PKDataCompetenza
    AND (
        @TipoData <> 'C'
        OR DC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
    )
WHERE (
    @NascondiOrdiniRinnovati = CAST(0 AS BIT)
    OR DOR.IDDocumento IS NULL
)
ORDER BY DO.AgenteAssegnato,
    DO.CodiceCliente,
    DO.NumeroDocumento;

SELECT
    RD.CodiceCliente,
    RD.RagioneSociale,
    RD.Indirizzo,
    RD.Citta,
    RD.Provincia,
    RD.PartitaIVA,
    RD.Agente,
    RD.AgenteAssegnato,
    RD.Azione,
    RD.Rinnovo,
    RD.PKDataInizioContratto,
    RD.DataInizioContratto,
    RD.PKDataFineContratto,
    RD.DataFineContratto,
    RD.PKDataDocumento,
    RD.DataDocumento,
    RD.NumeroDocumento,
    RD.CodicePagamento,
    RD.Pagamento,
    RD.Progetto,
    SUM(RD.TotaleDocumento) AS TotaleDocumento,
    SUM(RD.Insoluto) AS Insoluto,
    RD.PKDataDisdetta,
    RD.DataDisdetta,
    RD.MotivoDisdetta,
    RD.ProgressivoAgenteAssegnato,
    RD.ImportoProvvigioneCapoArea,
    RD.ImportoProvvigioneAgente,
    RD.ImportoProvvigioneSubagente,
    RD.DurataMesi,
    RD.Fatturazione,
    --RD.Progressivo,
    DENSE_RANK() OVER (ORDER BY RD.CodiceCliente) AS Progressivo,
    RD.Quote,
    RD.TipoCliente,
    RD.TipoFatturazione,
    RD.PKDataFattura,
    RD.NoteIntestazione,
    RD.Email,
    RD.ProvvigioneTeorica,
    RD.LiquidazioneProvvigioneTeorica,
    RD.AgenteProprietario,
    SUM(RD.TotaleRinnovo) AS TotaleRinnovo,
    RD.DataRinnovo,
    RD.TipoFatturazioneRinnovo,
    RD.DurataMesiRinnovo,
    RD.LivelloMIA

FROM #ReportData RD
GROUP BY RD.CodiceCliente,
    RD.RagioneSociale,
    RD.Indirizzo,
    RD.Citta,
    RD.Provincia,
    RD.PartitaIVA,
    RD.Agente,
    RD.AgenteAssegnato,
    RD.Azione,
    RD.Rinnovo,
    RD.PKDataInizioContratto,
    RD.DataInizioContratto,
    RD.PKDataFineContratto,
    RD.DataFineContratto,
    RD.PKDataDocumento,
    RD.DataDocumento,
    RD.NumeroDocumento,
    RD.CodicePagamento,
    RD.Pagamento,
    RD.Progetto,
    RD.PKDataDisdetta,
    RD.DataDisdetta,
    RD.MotivoDisdetta,
    RD.ProgressivoAgenteAssegnato,
    RD.ImportoProvvigioneCapoArea,
    RD.ImportoProvvigioneAgente,
    RD.ImportoProvvigioneSubagente,
    RD.DurataMesi,
    RD.Fatturazione,
    --RD.Progressivo,
    RD.Quote,
    RD.TipoCliente,
    RD.TipoFatturazione,
    RD.PKDataFattura,
    RD.NoteIntestazione,
    RD.Email,
    RD.ProvvigioneTeorica,
    RD.LiquidazioneProvvigioneTeorica,
    RD.AgenteProprietario,
    --RD.TotaleRinnovo,
    RD.DataRinnovo,
    RD.TipoFatturazioneRinnovo,
    RD.DurataMesiRinnovo,
    RD.LivelloMIA
ORDER BY RD.AgenteAssegnato,
    RD.CodiceCliente,
    RD.NumeroDocumento;

END;

GO

