/**
 * @storedprocedure Fact.usp_ReportDettaglioOrdini
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportDettaglioOrdini (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60)
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
    GROUP BY D.PKCliente
    HAVING SUM(S.ImportoResiduo) > 0.0
),
DettaglioOrdini
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
        D.NoteIntestazione AS Azione,
        D.RinnovoAutomatico AS Rinnovo,
        D.PKDataInizioContratto,
        DIC.Data_IT AS DataInizioContratto,
        D.PKDataFineContratto,
        DFC.Data_IT AS DataFineContratto,
        D.PKDataCompetenza AS PKDataDocumento,
        DC.Data_IT AS DataDocumento,
        D.NumeroDocumento,
        D.CondizioniPagamento AS Pagamento,
        D.Libero2 AS Progetto,
        A.Codice AS CodiceArticolo,
        A.Descrizione AS TipoAbbonamento,
        D.ImportoTotale AS TotaleDocumento,
        COALESCE(I.Insoluto, 0.0) AS Insoluto,
        C.PKDataDisdetta,
        DDIS.Data_IT AS DataDisdetta,
        C.MotivoDisdetta,
        D.NumeroRiga,
        ROW_NUMBER() OVER (PARTITION BY D.PKCliente ORDER BY D.PKDocumenti) AS rn,

        COALESCE(ICA.Prefisso, N'XXX') AS PrefissoCapoArea,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(ICA.Prefisso, N'XXX') ORDER BY C.CodiceCliente, D.NumeroDocumento, D.NumeroRiga) AS rnCapoArea,
        D.ImportoProvvigioneCapoArea,
        D.ImportoProvvigioneAgente,
        D.ImportoProvvigioneSubagente,
        A.Fatturazione,
        D.Progressivo

    FROM Fact.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
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
    INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND DC.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
    LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
    WHERE D.Profilo = N'ORDINE CLIENTE'
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
    DO.Pagamento,
    DO.Progetto,
    DO.CodiceArticolo,
    DO.TipoAbbonamento,
    DO.TotaleDocumento,
    CASE WHEN DO.rn = 1 THEN DO.Insoluto ELSE 0.0 END AS Insoluto,
    DO.PKDataDisdetta,
    DO.DataDisdetta,
    DO.MotivoDisdetta,
    DO.PrefissoCapoArea + RIGHT(N'000' + CONVERT(NVARCHAR(3), DO.rnCapoArea), 3) AS ProgressivoAgenteAssegnato,
    DO.ImportoProvvigioneCapoArea,
    DO.ImportoProvvigioneAgente,
    DO.ImportoProvvigioneSubagente,
    DATEDIFF(MONTH, DO.PKDataInizioContratto, DO.PKDataFineContratto) AS DurataMesi,
    DO.Fatturazione,
    DO.Progressivo

FROM DettaglioOrdini DO
ORDER BY DO.AgenteAssegnato,
    DO.CodiceCliente,
    DO.NumeroDocumento,
    DO.NumeroRiga;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportDettaglioOrdini TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE;
DECLARE @PKDataFinePeriodo DATE;
DECLARE @GruppoAgenti NVARCHAR(60);
DECLARE @CapoArea NVARCHAR(60);

EXEC Fact.usp_ReportDettaglioOrdini
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @PKDataFinePeriodo = @PKDataFinePeriodo,
    @GruppoAgenti = @GruppoAgenti,
    @CapoArea = @CapoArea;
GO
