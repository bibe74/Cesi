/**
 * @storedprocedure Fact.usp_ReportCruscottoClienti
*/

CREATE   PROCEDURE Fact.usp_ReportCruscottoClienti (
    @PKDataFinePeriodo DATE = NULL,
    --@GruppoAgenti NVARCHAR(60) = NULL,
    @CapoArea NVARCHAR(60) = NULL,
    @HasAbbonamentoMySolution BIT = NULL,
    @HasAbbonamentoMIA BIT = NULL
)
AS
BEGIN

SET NOCOUNT ON;

--DECLARE @PKDataFinePeriodo DATE;

SET @PKDataFinePeriodo = COALESCE(@PKDataFinePeriodo, DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP)));

DECLARE @PKDataInizioPeriodo DATE,
    @PKDataInizioUltimoMese DATE,
    @PKDataInizioUltimoTrimestre DATE,
    @PKDataInizioUltimoSemestre DATE,
    @AnnoCorrente INT,
    ----@CodiceEsercizioMasterCorrente NVARCHAR(10),
    ----@CodiceEsercizioMasterPrecedente NVARCHAR(10);
    @AnnoMasterCorrente SMALLINT,
    @AnnoMasterPrecedente SMALLINT,
    @DataInizioAnnoCorrente DATE,
    @DataFineAnnoCorrente DATE;

SELECT @PKDataInizioUltimoSemestre = DATEADD(MONTH, -6, DATEADD(DAY, 1, @PKDataFinePeriodo)),
    @PKDataInizioUltimoTrimestre = DATEADD(MONTH, -3, DATEADD(DAY, 1, @PKDataFinePeriodo)),
    @PKDataInizioUltimoMese = DATEADD(MONTH, -1, DATEADD(DAY, 1, @PKDataFinePeriodo));

SET @PKDataInizioPeriodo = @PKDataInizioUltimoSemestre;

SELECT @AnnoCorrente = YEAR(@PKDataFinePeriodo);

----SELECT @CodiceEsercizioMasterCorrente = CONVERT(NVARCHAR(4), @AnnoCorrente) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente + 1),
----    @CodiceEsercizioMasterPrecedente = CONVERT(NVARCHAR(4), @AnnoCorrente - 1) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente);
SELECT @AnnoMasterCorrente = @AnnoCorrente,
    @AnnoMasterPrecedente = @AnnoCorrente - 1,
    @DataInizioAnnoCorrente = DATEFROMPARTS(@AnnoCorrente, 1, 1),
    @DataFineAnnoCorrente = DATEFROMPARTS(@AnnoCorrente, 12, 31);

--SELECT @PKDataInizioPeriodo, @PKDataFinePeriodo, @PKDataInizioUltimoMese, @PKDataInizioUltimoTrimestre, @PKDataInizioUltimoSemestre, @AnnoCorrente, @AnnoMasterCorrente, @AnnoMasterPrecedente;

DECLARE @AgenteProprietarioPrefix NVARCHAR(20) = N'Proprietario(';

DROP TABLE IF EXISTS #DettaglioOrdini;

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
Ordini
AS (
    SELECT
        D.IDDocumento,
        D.NumeroDocumento,
        C.PKCliente,
        CASE WHEN D.NoteDecisionali LIKE @AgenteProprietarioPrefix + N'%)' THEN SUBSTRING(D.NoteDecisionali, LEN(@AgenteProprietarioPrefix)+1, LEN(D.NoteDecisionali) - LEN(@AgenteProprietarioPrefix) - 1) ELSE GA.CapoArea END AS AgenteProprietario,

        -- Abbonamento MySolution
        A.Tipo AS TipoAbbonamento,
        A.MacroTipoAbbonamento,
		SUM(CASE WHEN D.NumeroRiga = 1 THEN D.Quote ELSE NULL END) AS QuoteFormazione,
        D.Libero1 AS Azione,
        D.RinnovoAutomatico AS ClausolaRinnovoAutomatico,
        D.PKDataInizioContratto,
        DIC.Data_IT AS DataInizioContratto,
        D.PKDataFineContratto,
        DFC.Data_IT AS DataFineContratto,
        D.PKDataCompetenza,
        SUM(D.ImportoTotale) AS TotaleDocumento,
        COALESCE(I.Insoluto, 0.0) AS Insoluto

    FROM Fact.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
        AND GA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN IMPORT.CapiArea ICA ON ICA.CapoArea = GA.CapoArea
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
        AND GA.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Data DIC ON DIC.PKData = D.PKDataInizioContratto
    INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia
        AND MT.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
    WHERE D.Profilo = N'ORDINE CLIENTE'
        AND (
            D.PKDataFineContratto >= @PKDataInizioPeriodo
            AND D.PKDataInizioContratto <= @PKDataFinePeriodo
        )
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY D.IDDocumento,
        D.NumeroDocumento,
        C.PKCliente,
        CASE WHEN D.NoteDecisionali LIKE @AgenteProprietarioPrefix + N'%)' THEN SUBSTRING(D.NoteDecisionali, LEN(@AgenteProprietarioPrefix)+1, LEN(D.NoteDecisionali) - LEN(@AgenteProprietarioPrefix) - 1) ELSE GA.CapoArea END,
        A.Tipo,
        A.MacroTipoAbbonamento,
        D.Libero1,
        D.RinnovoAutomatico,
        D.PKDataInizioContratto,
        DIC.Data_IT,
        D.PKDataFineContratto,
        DFC.Data_IT,
        D.PKDataCompetenza,
        COALESCE(I.Insoluto, 0.0)
),
ElencoOrdini
AS (
    SELECT
        O.IDDocumento,
        O.NumeroDocumento,
        O.PKCliente,
        O.AgenteProprietario,
        O.TipoAbbonamento,
        O.MacroTipoAbbonamento,
        O.QuoteFormazione,
        O.Azione,
        O.ClausolaRinnovoAutomatico,
        O.PKDataInizioContratto,
        O.DataInizioContratto,
        O.PKDataFineContratto,
        O.DataFineContratto,
        O.PKDataCompetenza,
        O.TotaleDocumento,
        O.Insoluto,
        ROW_NUMBER() OVER (PARTITION BY O.PKCliente, O.MacroTipoAbbonamento ORDER BY O.PKDataInizioContratto DESC, O.NumeroDocumento DESC) AS rn

    FROM Ordini O
    WHERE O.PKDataInizioContratto <= @DataFineAnnoCorrente
        AND O.PKDataFineContratto >= @DataInizioAnnoCorrente
)
SELECT
    *

INTO #DettaglioOrdini
FROM ElencoOrdini EO
WHERE EO.rn = 1;

CREATE NONCLUSTERED INDEX IX_DettaglioOrdini_PKCliente_MacroTipoAbbonamento ON #DettaglioOrdini (PKCliente, MacroTipoAbbonamento);

WITH CreditiMIA
AS (
    SELECT
        C.PKCliente,
        --C.Email,
        SUM(COAI.CreditiAcquistati) AS CreditiAcquistati,
        SUM(COAI.CreditiConsumati) AS CreditiConsumati,
        SUM(COAI.CreditiFuoriOrdine) AS CreditiFuoriOrdine,
        SUM(COAI.CreditiResidui) AS CreditiResidui

    FROM Fact.CreditiOpenAI COAI
    INNER JOIN Dim.Cliente C ON C.PKCliente = COAI.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    WHERE COAI.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.PKCliente
        --, C.Email
),
Accessi
AS (
    SELECT
        C.PKCliente, --C.Email,
        SUM(CASE WHEN A.PKData BETWEEN @PKDataInizioUltimoMese AND @PKDataFinePeriodo THEN A.NumeroAccessi ELSE NULL END) AS AccessiUltimoMese,
        SUM(CASE WHEN A.PKData BETWEEN @PKDataInizioUltimoTrimestre AND @PKDataFinePeriodo THEN A.NumeroAccessi ELSE NULL END) AS AccessiUltimoTrimestre,
        SUM(CASE WHEN A.PKData BETWEEN @PKDataInizioUltimoSemestre AND @PKDataFinePeriodo THEN A.NumeroAccessi ELSE NULL END) AS AccessiUltimoSemestre

    FROM Fact.Accessi A
    INNER JOIN Dim.Data D ON D.PKData = A.PKData
        AND A.PKData BETWEEN @PKDataInizioUltimoSemestre AND @PKDataFinePeriodo
    INNER JOIN Dim.ClienteAccessi CA ON CA.PKClienteAccessi = A.PKCliente
        AND CA.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Cliente C ON C.PKCliente = CA.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    WHERE A.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.PKCliente --, C.Email
),
IscrizioniMaster
AS (
    SELECT
        D.PKCliente, --C.Email,
        ACM.CategoriaMaster,
        ----ACM.CodiceEsercizioMaster AS CodiceEsercizio,
        DD.Anno AS AnnoMaster,
        MAX(D.PKDataDocumento) AS DataUltimaFattura,
        COUNT(1) AS NumeroIscritti,
        SUM(D.ImportoTotale * ACM.Percentuale) AS ImportoTotale

    FROM Fact.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        AND A.IsDeleted = CAST(0 AS BIT)
        ----AND A.CodiceEsercizioMaster IN (@CodiceEsercizioMasterCorrente, @CodiceEsercizioMasterPrecedente)
    INNER JOIN Import.ArticoloCategoriaMaster ACM ON ACM.Codice = A.Codice
    INNER JOIN Dim.Data DD ON DD.PKData = D.PKDataDocumento
        AND DD.Anno IN (@AnnoMasterPrecedente, @AnnoMasterCorrente)
    WHERE D.IDProfilo = N'ORDSEM'
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY D.PKCliente, --C.Email,
        ACM.CategoriaMaster,
        ----ACM.CodiceEsercizioMaster,
        DD.Anno
),
Clienti
AS (
    SELECT
        C.PKCliente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.Email,
        C.Localita AS Citta,
        C.Provincia,
        C.PKDataDisdetta,
        DDIS.Data_IT AS DataDisdetta,
        C.Telefono

    FROM Dim.Cliente C
    INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        --AND (
        --    @GruppoAgenti IS NULL
        --    OR GA.GruppoAgenti = @GruppoAgenti
        --)
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
        AND (
            @HasAbbonamentoMySolution IS NULL
            OR C.HasAbbonamentoMySolution = @HasAbbonamentoMySolution
        )
        AND (
            @HasAbbonamentoMIA IS NULL
            OR C.HasAbbonamentoMIA = @HasAbbonamentoMIA
        )
        AND GA.IsDeleted = CAST(0 AS BIT)
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND EXISTS(SELECT DO.IDDocumento FROM #DettaglioOrdini DO WHERE DO.PKCliente = C.PKCliente)
)
SELECT
    C.PKCliente,
    C.CodiceCliente,
    C.RagioneSociale,
    C.Email,
    C.Citta,
    C.Provincia,
    C.PKDataDisdetta,
    C.DataDisdetta,
    C.Telefono,

    DOMYS.IDDocumento,
    --DO.NumeroDocumento,
    DOMYS.AgenteProprietario,
    DOMYS.QuoteFormazione,
    DOMYS.Azione,
    DOMYS.ClausolaRinnovoAutomatico,
    DOMYS.PKDataInizioContratto,
    DOMYS.DataInizioContratto,
    DOMYS.PKDataFineContratto,
    DOMYS.DataFineContratto,
    DOMYS.TotaleDocumento,
    CASE WHEN DOMYS.rn = 1 THEN DOMYS.Insoluto ELSE 0.0 END AS Insoluto,
    A.AccessiUltimoMese,
    A.AccessiUltimoTrimestre,
    A.AccessiUltimoSemestre,
    DOMIA.PKDataInizioContratto AS PKDataInizioContrattoMIA,
    DOMIA.DataInizioContratto AS DataInizioContrattoMIA,
    DOMIA.PKDataFineContratto AS PKDataFineContrattoMIA,
    DOMIA.DataFineContratto AS DataFineContrattoMIA,
    DOMIA.TotaleDocumento AS TotaleDocumentoMIA,
    CMIA.CreditiAcquistati,
    --CMIA.CreditiConsumati,
    --CMIA.CreditiFuoriOrdine,
    CMIA.CreditiResidui,
    ----@CodiceEsercizioMasterPrecedente AS CodiceEsercizioPrecedente,
    CONVERT(NVARCHAR(4), @AnnoMasterPrecedente) AS CodiceEsercizioPrecedente,
    IMAP.NumeroIscritti AS NumeroIscrittiAnnoPrecedente,
    IMAP.ImportoTotale AS ImportoTotaleAnnoPrecedente,
    ----@CodiceEsercizioMasterCorrente AS CodiceEsercizioCorrente,
    CONVERT(NVARCHAR(4), @AnnoMasterCorrente) AS CodiceEsercizioCorrente,
    IMMAP.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoPrecedente,
    IMMAP.ImportoTotale AS ImportoTotaleMiniMasterAnnoPrecedente,
    IMAC.NumeroIscritti AS NumeroIscrittiAnnoCorrente,
    IMAC.ImportoTotale AS ImportoTotaleAnnoCorrente,
    IMMAC.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoCorrente,
    IMMAC.ImportoTotale AS ImportoTotaleMiniMasterAnnoCorrente

FROM Clienti C
LEFT JOIN #DettaglioOrdini DOMYS ON DOMYS.PKCliente = C.PKCliente
    AND DOMYS.MacroTipoAbbonamento = N'MYS'
LEFT JOIN #DettaglioOrdini DOMIA ON DOMIA.PKCliente = C.PKCliente
    AND DOMIA.MacroTipoAbbonamento = N'MIA'
INNER JOIN Dim.Data DC ON DC.PKData = DOMYS.PKDataCompetenza
LEFT JOIN Accessi A ON A.PKCliente = C.PKCliente
LEFT JOIN CreditiMIA CMIA ON CMIA.PKCliente = C.PKCliente
LEFT JOIN IscrizioniMaster IMAC ON IMAC.PKCliente = C.PKCliente
    AND IMAC.CategoriaMaster = N'Master MySolution'
    --AND IMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    AND IMAC.AnnoMaster = @AnnoMasterCorrente
LEFT JOIN IscrizioniMaster IMAP ON IMAP.PKCliente = C.PKCliente
    AND IMAP.CategoriaMaster = N'Master MySolution'
    ----AND IMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente
    AND IMAP.AnnoMaster = @AnnoMasterPrecedente
LEFT JOIN IscrizioniMaster IMMAC ON IMMAC.PKCliente = C.PKCliente
    AND IMMAC.CategoriaMaster = N'Mini Master Revisione'
    ----AND IMMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    AND IMMAC.AnnoMaster = @AnnoMasterCorrente
LEFT JOIN IscrizioniMaster IMMAP ON IMMAP.PKCliente = C.PKCliente
    AND IMMAP.CategoriaMaster = N'Mini Master Revisione'
    ----AND IMMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente
    AND IMMAP.AnnoMaster = @AnnoMasterPrecedente
ORDER BY DOMYS.AgenteProprietario,
    C.CodiceCliente,
    DOMYS.rn;

END;

GO

