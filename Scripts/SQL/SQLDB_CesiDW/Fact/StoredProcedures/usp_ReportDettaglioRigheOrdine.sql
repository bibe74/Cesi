
/**
 * @storedprocedure Fact.usp_ReportDettaglioRigheOrdine
*/

CREATE   PROCEDURE Fact.usp_ReportDettaglioRigheOrdine (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60),
    --@TipoFiltroData CHAR(1) = 'M', -- 'M': mese, 'P': periodo
    @TipoData CHAR(1) = 'C', -- 'C': competenza (data ordine), 'I': inizio contratto, 'F': fine contratto
    @RagioneSociale NVARCHAR(120) = NULL,
    @CodiceCliente NVARCHAR(10) = NULL,
    @PartitaIVA NVARCHAR(20) = NULL
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

WITH Insoluti
AS (
    SELECT
        D.PKCliente,
        SUM(S.ImportoResiduo) AS Insoluto
    FROM Fact.Scadenze S
    INNER JOIN Fact.Documenti D ON D.PKDocumenti = S.PKDocumenti
        AND D.IsDeleted = CAST(0 AS BIT)
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
        C.CodiceCliente,
        C.RagioneSociale,
        C.Indirizzo,
        C.Localita AS Citta,
        C.Provincia,
        C.PartitaIVA,
        GA.GruppoAgenti AS Agente,
        GA.CapoArea AS AgenteAssegnato,
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
        A.Tipo AS TipoCliente,
        D.TipoFatturazione,
        COALESCE(MAX(ROF.PKDataFattura), CAST('19000101' AS DATE)) AS PKDataFattura,
        D.NoteIntestazione,
        C.Email,
        COALESCE(CASE MT.MacroTipologia
          WHEN N'Nuova vendita' THEN ICA.ProvvigioneNuovo
          WHEN N'Rinnovo' THEN ICA.ProvvigioneRinnovo
          ELSE NULL
        END, 0.0) / 100.0 AS ProvvigioneTeorica,
        N'' AS LiquidazioneProvvigioneTeorica,
        A.Codice AS CodiceArticolo,
        A.Descrizione AS DescrizioneArticolo,
        D.IDDocumento,
        ROW_NUMBER() OVER (PARTITION BY D.IDDocumento ORDER BY D.IDDocumento_Riga) AS rn

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
        AND (
            @TipoData <> 'I'
            OR DIC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        )
    INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
        AND (
            @TipoData <> 'F'
            OR DFC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        )
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND (
            @TipoData <> 'C'
            OR DC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        )
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
    INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia
    LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
    LEFT JOIN RigaOrdineFattura ROF ON ROF.IDDocumento_Riga = D.IDDocumento_Riga
    WHERE D.Profilo = N'ORDINE CLIENTE'
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY COALESCE (I.Insoluto, 0.0),
        COALESCE (ICA.Prefisso, N'XXX'),
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
        A.Tipo,
        D.TipoFatturazione,
        D.NoteIntestazione,
        C.Email,
        MT.MacroTipologia,
        ICA.ProvvigioneNuovo,
        ICA.ProvvigioneRinnovo,
        A.Codice,
        A.Descrizione,
        D.IDDocumento,
        D.IDDocumento_Riga
),
DettaglioOrdini
AS (
    SELECT
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
        CASE WHEN O.rn = 1 THEN O.Insoluto ELSE NULL END AS Insoluto,
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
        O.CodiceArticolo,
        O.DescrizioneArticolo

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
    DO.PrefissoCapoArea + RIGHT(N'000' + CONVERT(NVARCHAR(3), DO.rnCapoArea), 3) AS ProgressivoAgenteAssegnato,
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
    DO.CodiceArticolo,
    DO.DescrizioneArticolo

FROM DettaglioOrdini DO
ORDER BY DO.AgenteAssegnato,
    DO.CodiceCliente,
    DO.NumeroDocumento;

END;

GO

