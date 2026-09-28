/**
 * @storedprocedure Fact.usp_ReportDettaglioFatture
*/

CREATE   PROCEDURE Fact.usp_ReportDettaglioFatture (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60),
    --@TipoFiltroData CHAR(1) = 'M', -- 'M': mese, 'P': periodo
    @TipoData CHAR(1) = 'C', -- 'C': competenza (data ordine), 'I': inizio contratto, 'F': fine contratto
    @RagioneSociale NVARCHAR(120) = NULL,
    @CodiceCliente NVARCHAR(10) = NULL,
    @PartitaIVA NVARCHAR(20) = NULL,
    @TipoReport CHAR(1) = NULL, -- 'F': Formazione, 'M': My Solution
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
--RigaOrdineFattura
--AS (
--    SELECT
--        O.IDDocumento_Riga,
--        MAX(F.PKDataCompetenza) AS PKDataFattura
--    FROM Fact.Documenti O
--    INNER JOIN Fact.Documenti F ON F.IDDocumento_Riga_Provenienza = O.IDDocumento_Riga
--    WHERE O.Profilo = N'ORDINE CLIENTE'
--    GROUP BY O.IDDocumento_Riga
--),
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
        --CASE WHEN GA.CapoArea = N'' THEN C.CapoAreaDefault ELSE GA.CapoArea END AS AgenteAssegnato,
        GA.CapoArea AS AgenteAssegnato,
        D.Libero1 AS Azione,
        D.RinnovoAutomatico AS Rinnovo,
        DP.PKDataInizioContratto,
        DIC.Data_IT AS DataInizioContratto,
        DP.PKDataFineContratto,
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
        ----COALESCE(MAX(ROF.PKDataFattura), CAST('19000101' AS DATE)) AS PKDataFattura,
        D.NoteIntestazione,
        C.Email

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
    INNER JOIN Fact.Documenti DP ON DP.IDDocumento_Riga = D.IDDocumento_Riga_Provenienza
    INNER JOIN Dim.Data DIC ON DIC.PKData = DP.PKDataInizioContratto
        AND (
            @TipoData <> 'I'
            OR DIC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
        )
    INNER JOIN Dim.Data DFC ON DFC.PKData = DP.PKDataFineContratto
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
        AND (
            @TipoReport IS NULL
            OR @TipoReport = 'F'
            OR (@TipoReport = 'M' AND A.Tipo <> N'')
        )
    INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
    LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
    WHERE D.Profilo LIKE N'FATTURA%'
        AND D.IsDeleted = CAST(0 AS BIT)
        AND (
            @TipoReport IS NULL
            OR (@TipoReport = 'F' AND D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT))
            OR @TipoReport = 'M'
        )
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
        DP.PKDataInizioContratto,
        DIC.Data_IT,
        DP.PKDataFineContratto,
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
        C.Email
        --, C.CapoAreaDefault
),
DettaglioFatture
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
        ROW_NUMBER() OVER (PARTITION BY O.PrefissoCapoArea ORDER BY O.CodiceCliente, O.NumeroDocumento) AS rnCapoArea,
        O.TipoCliente,
        O.TipoFatturazione,
        ----O.PKDataFattura,
        O.NoteIntestazione,
        O.Email

    FROM Ordini O
)
SELECT
    DF.CodiceCliente,
    DF.RagioneSociale,
    DF.Indirizzo,
    DF.Citta,
    DF.Provincia,
    DF.PartitaIVA,
    DF.Agente,
    DF.AgenteAssegnato,
    DF.Azione,
    DF.Rinnovo,
    DF.PKDataInizioContratto,
    DF.DataInizioContratto,
    DF.PKDataFineContratto,
    DF.DataFineContratto,
    DF.PKDataDocumento,
    DF.DataDocumento,
    DF.NumeroDocumento,
    DF.CodicePagamento,
    DF.Pagamento,
    DF.Progetto,
    --DO.CodiceArticolo,
    --DO.TipoAbbonamento,
    DF.TotaleDocumento,
    CASE WHEN DF.rn = 1 THEN DF.Insoluto ELSE 0.0 END AS Insoluto,
    DF.PKDataDisdetta,
    DF.DataDisdetta,
    DF.MotivoDisdetta,
    DF.PrefissoCapoArea + RIGHT(N'0000' + CONVERT(NVARCHAR(4), DF.rnCapoArea), 4) AS ProgressivoAgenteAssegnato,
    DF.ImportoProvvigioneCapoArea,
    DF.ImportoProvvigioneAgente,
    DF.ImportoProvvigioneSubagente,
    DATEDIFF(MONTH, DF.PKDataInizioContratto, DATEADD(DAY, 1, DF.PKDataFineContratto)) AS DurataMesi,
    DF.Fatturazione,
    DF.Progressivo,
	DF.Quote,
    DF.TipoCliente,
    DF.TipoFatturazione,
    ----DF.PKDataFattura,
    DF.NoteIntestazione,
    DF.Email

FROM DettaglioFatture DF
ORDER BY DF.AgenteAssegnato,
    DF.CodiceCliente,
    DF.NumeroDocumento;

END;

GO

