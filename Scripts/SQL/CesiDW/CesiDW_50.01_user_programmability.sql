USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @storedprocedure Fact.usp_ReportAccessi
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportAccessi (
    @PKDataInizioPeriodo DATE = NULL,
    @Agente NVARCHAR(60) = NULL,
    @TipoCliente NVARCHAR(10) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    SET DATEFIRST 1;

    DECLARE @PKDataFinePeriodo DATE;

    IF (@PKDataInizioPeriodo IS NULL)
    BEGIN

        DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE));

        SET @PKDataFinePeriodo = DATEADD(DAY, 7-DATEPART(WEEKDAY, @Yesterday), @Yesterday);

        SELECT @PKDataInizioPeriodo = DATEADD(DAY, -27, @PKDataFinePeriodo);
    END;

    SELECT @PKDataFinePeriodo = DATEADD(DAY, 27, @PKDataInizioPeriodo);

    WITH SettimaneNumerate
    AS (
        SELECT
            D.PKData AS PKDataLunedi,
            DATEADD(DAY, 6, D.PKData) AS PKDataDomenica,
            LEFT(CONVERT(NVARCHAR(2), ROW_NUMBER() OVER (ORDER BY D.PKData DESC)) + '^ sett. ' + CONVERT(NVARCHAR(10), D.PKData, 103), 14) AS DescrizioneSettimana,
            ROW_NUMBER() OVER (ORDER BY D.PKData DESC) AS rn

        FROM Dim.Data D
        WHERE D.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
            AND DATEPART(WEEKDAY, D.PKData) = 1
    ),
    AccessiSettimaneNumerate
    AS (
        SELECT
            C.PKCliente,
            SN.rn,
            SN.DescrizioneSettimana,
            SUM(A.NumeroAccessi) AS NumeroAccessi,
            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
            COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

        FROM Fact.Accessi A
        INNER JOIN Dim.ClienteAccessi CA ON CA.PKClienteAccessi = A.PKCliente
        INNER JOIN Dim.Cliente C ON C.PKCliente = CA.PKCliente
        INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
            AND (
                @Agente IS NULL
                OR GA.CapoArea = @Agente
            )
        INNER JOIN SettimaneNumerate SN ON A.PKData BETWEEN SN.PKDataLunedi AND SN.PKDataDomenica
        WHERE A.IsDeleted = CAST(0 AS BIT)
        GROUP BY C.PKCliente,
            SN.rn,
            SN.DescrizioneSettimana
    ),
    AccessiUltimiTreMesi
    AS (
        SELECT
            C.PKCliente,
            SUM(A.NumeroAccessi) AS NumeroAccessi,
            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
            COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

        FROM Fact.Accessi A
        INNER JOIN Dim.ClienteAccessi CA ON CA.PKClienteAccessi = A.PKCliente
            AND (
                @Agente IS NULL
                OR CA.Agente = @Agente
            )
        INNER JOIN Dim.Cliente C ON C.PKCliente = CA.PKCliente
        --INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        WHERE A.IsDeleted = CAST(0 AS BIT)
            AND A.PKData BETWEEN DATEADD(MONTH, -3, @PKDataFinePeriodo) AND @PKDataFinePeriodo
        GROUP BY C.PKCliente
    ),
    Clienti
    AS (
        SELECT
            C.PKCliente,
            C.CodiceCliente,
            C.Agente,
            C.RagioneSociale,
            C.Email,
            C.Telefono,
            C.TipoCliente,
            C.Localita AS Comune,
            C.IDProvincia AS Provincia,
            C.Regione,
            DIC.Data_IT AS DataInizio,
            DFC.Data_IT AS DataScadenza

        FROM Dim.ClienteAccessi C
        INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        INNER JOIN Dim.Data DIC ON DIC.PKData = C.PKDataInizioContratto
        INNER JOIN Dim.Data DFC ON DFC.PKData = C.PKDataFineContratto
        WHERE C.IsDeleted = CAST(0 AS BIT)
            AND C.IsAbbonato = CAST(1 AS BIT)
            AND C.PKDataFineContratto >= @PKDataInizioPeriodo
            AND (
                @TipoCliente IS NULL
                OR C.TipoCliente = @TipoCliente
            )
            AND (
                @Agente IS NULL
                OR C.Agente = @Agente
            )
    )
    SELECT
        C.Agente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.Email,
        C.Telefono,
        C.Comune,
        C.Provincia,
        C.Regione,
        C.DataInizio,
        C.DataScadenza,
        SN.DescrizioneSettimana,
        COALESCE(ASN.NumeroAccessi, 0) AS NumeroAccessi,
        COALESCE(ASN.NumeroPagineVisitate, 0) AS NumeroPagineVisitate,
        COALESCE(ASN.NumeroGiorniAccesso, 0) AS NumeroGiorniAccesso,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY C.PKCliente ORDER BY SN.rn) = 1 THEN COALESCE(AU3M.NumeroGiorniAccesso, 0) ELSE 0 END AS NumeroGiorniAccessoUltimiTreMesi

    FROM Clienti C
    CROSS JOIN SettimaneNumerate SN
    LEFT JOIN AccessiSettimaneNumerate ASN ON ASN.PKCliente = C.PKCliente AND ASN.rn = SN.rn
    LEFT JOIN AccessiUltimiTreMesi AU3M ON AU3M.PKCliente = C.PKCliente

    WHERE C.PKCliente > 0

    ORDER BY C.Agente,
        C.RagioneSociale,
        SN.rn;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportAccessi TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE;
DECLARE @Agente NVARCHAR(60);
DECLARE @TipoCliente NVARCHAR(10);

--EXEC Fact.usp_ReportAccessi
--    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
--    @Agente = @Agente,
--    @TipoCliente = @TipoCliente;

EXEC Fact.usp_ReportAccessi
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @Agente = N'AMADIO ERNESTO',
    @TipoCliente = @TipoCliente;
GO

/**
 * @storedprocedure Fact.usp_ReportAccessiDemo
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportAccessiDemo (
    @PKDataInizioPeriodo DATE = NULL,
    @Agente NVARCHAR(60) = NULL,
    @TipoCliente NVARCHAR(10) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    SET DATEFIRST 1;

    DECLARE @PKDataFinePeriodo DATE;

    IF (@PKDataInizioPeriodo IS NULL)
    BEGIN

        DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE));

        SET @PKDataFinePeriodo = DATEADD(DAY, 7-DATEPART(WEEKDAY, @Yesterday), @Yesterday);

        SELECT @PKDataInizioPeriodo = DATEADD(DAY, -27, @PKDataFinePeriodo);
    END;

    SELECT @PKDataFinePeriodo = DATEADD(DAY, 27, @PKDataInizioPeriodo);

    WITH SettimaneNumerate
    AS (
        SELECT
            D.PKData AS PKDataLunedi,
            DATEADD(DAY, 6, D.PKData) AS PKDataDomenica,
            LEFT(CONVERT(NVARCHAR(2), ROW_NUMBER() OVER (ORDER BY D.PKData DESC)) + '^ sett. ' + CONVERT(NVARCHAR(10), D.PKData, 103), 14) AS DescrizioneSettimana,
            ROW_NUMBER() OVER (ORDER BY D.PKData DESC) AS rn

        FROM Dim.Data D
        WHERE D.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
            AND DATEPART(WEEKDAY, D.PKData) = 1
    ),
    AccessiSettimaneNumerate
    AS (
        SELECT
            C.PKCliente,
            SN.rn,
            SN.DescrizioneSettimana,
            SUM(A.NumeroAccessi) AS NumeroAccessi,
            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
            COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

        FROM Fact.Accessi A
        INNER JOIN Dim.ClienteAccessi CA ON CA.PKClienteAccessi = A.PKCliente
            AND (
                @Agente IS NULL
                OR CA.Agente = @Agente
            )
        INNER JOIN Dim.Cliente C ON C.PKCliente = CA.PKCliente
        --INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        INNER JOIN SettimaneNumerate SN ON A.PKData BETWEEN SN.PKDataLunedi AND SN.PKDataDomenica
        WHERE A.IsDeleted = CAST(0 AS BIT)
        GROUP BY C.PKCliente,
            SN.rn,
            SN.DescrizioneSettimana
    ),
    AccessiUltimiTreMesi
    AS (
        SELECT
            C.PKCliente,
            SUM(A.NumeroAccessi) AS NumeroAccessi,
            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
            COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

        FROM Fact.Accessi A
        INNER JOIN Dim.ClienteAccessi CA ON CA.PKClienteAccessi = A.PKCliente
        INNER JOIN Dim.Cliente C ON C.PKCliente = CA.PKCliente
        INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
            AND (
                @Agente IS NULL
                OR GA.CapoArea = @Agente
            )
        WHERE A.IsDeleted = CAST(0 AS BIT)
            AND A.PKData BETWEEN DATEADD(MONTH, -3, @PKDataFinePeriodo) AND @PKDataFinePeriodo
        GROUP BY C.PKCliente
    ),
    Clienti
    AS (
        SELECT
            C.PKCliente,
            C.CodiceCliente,
            C.Agente,
            C.RagioneSociale,
            C.Email,
            C.Telefono,
            C.TipoCliente,
            C.Localita AS Comune,
            C.IDProvincia AS Provincia,
            C.Regione,
            DIC.Data_IT AS DataInizio,
            DFC.Data_IT AS DataScadenza

        FROM Dim.ClienteAccessi C
        INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        INNER JOIN Dim.Data DIC ON DIC.PKData = C.PKDataInizioContratto
        INNER JOIN Dim.Data DFC ON DFC.PKData = C.PKDataFineContratto
        WHERE C.IsDeleted = CAST(0 AS BIT)
            AND C.HasRoleMySolutionDemo = CAST(1 AS BIT)
            AND (
                @TipoCliente IS NULL
                OR C.TipoCliente = @TipoCliente
            )
            AND (
                @Agente IS NULL
                OR C.Agente = @Agente
            )
    )
    SELECT
        C.Agente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.Email,
        C.Telefono,
        C.Comune,
        C.Provincia,
        C.Regione,
        C.DataInizio,
        C.DataScadenza,
        SN.DescrizioneSettimana,
        COALESCE(ASN.NumeroAccessi, 0) AS NumeroAccessi,
        COALESCE(ASN.NumeroPagineVisitate, 0) AS NumeroPagineVisitate,
        COALESCE(ASN.NumeroGiorniAccesso, 0) AS NumeroGiorniAccesso,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY C.PKCliente ORDER BY SN.rn) = 1 THEN COALESCE(AU3M.NumeroGiorniAccesso, 0) ELSE 0 END AS NumeroGiorniAccessoUltimiTreMesi

    FROM Clienti C
    CROSS JOIN SettimaneNumerate SN
    LEFT JOIN AccessiSettimaneNumerate ASN ON ASN.PKCliente = C.PKCliente AND ASN.rn = SN.rn
    LEFT JOIN AccessiUltimiTreMesi AU3M ON AU3M.PKCliente = C.PKCliente

    WHERE C.PKCliente > 0

    ORDER BY C.Agente,
        C.RagioneSociale,
        SN.rn;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportAccessiDemo TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE;
DECLARE @Agente NVARCHAR(60);
DECLARE @TipoCliente NVARCHAR(10);

--EXEC Fact.usp_ReportAccessiDemo
--    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
--    @Agente = @Agente,
--    @TipoCliente = @TipoCliente;

EXEC Fact.usp_ReportAccessiDemo
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @Agente = @Agente,
    @TipoCliente = @TipoCliente;
GO

/*

SET DATEFIRST 1;

DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE));

DECLARE @LastSunday DATE = DATEADD(DAY, 7-DATEPART(WEEKDAY, @Yesterday), @Yesterday);

WITH Last60Periods
AS (
    SELECT TOP (60)
        DATEADD(DAY, -7*4+1, D.PKData) AS DateFirstMonday,
        D.PKData AS DateLastSunday,
        D.AnnoSettimana

    FROM Dim.Data D
    WHERE D.PKData <= @LastSunday
        AND DATEPART(WEEKDAY, D.PKData) = 7
    ORDER BY D.PKData DESC
)
SELECT
    L60.AnnoSettimana AS AnnoSettimanaInizioPeriodo,
    L60.DateFirstMonday AS PKDataInizioPeriodo,
    L60.DateLastSunday AS PKDataFinePeriodo,
    CONVERT(NVARCHAR(10), L60.DateFirstMonday, 103) + N' - ' + CONVERT(NVARCHAR(10), L60.DateLastSunday, 103) AS DescrizionePeriodo

FROM Last60Periods L60
ORDER BY PKDataInizioPeriodo DESC;
GO

SELECT DISTINCT
    C.Agente,
    C.Agente AS DescrizioneAgente

FROM Fact.Accessi A
INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
WHERE A.PKCliente > 0

UNION ALL SELECT NULL, N'Tutti'

ORDER BY Agente;
GO

SELECT DISTINCT
       C.Agente,
       C.Agente AS AgenteDescrizione

FROM Fact.Accessi A
INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = GA.CapoArea
    AND AUCA.ADUser = @ADUser

UNION ALL

SELECT
    NULL,
    N'Tutti'

FROM Import.Amministratori A
WHERE A.ADUser = @ADUser
ORDER BY Agente;
GO

SELECT DISTINCT
    C.Agente,
    C.Agente AS AgenteDescrizione

FROM Fact.Accessi A
INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = GA.CapoArea
    AND AUCA.ADUser = @ADUser

UNION ALL SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY Agente;
GO

SELECT DISTINCT
    C.TipoCliente,
    C.TipoCliente AS DescrizioneTipoCliente

FROM Fact.Accessi A
INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
WHERE A.PKCliente > 0

UNION ALL SELECT NULL, N'Tutti'

ORDER BY TipoCliente;
GO

*/

/**
 * @storedprocedure Fact.usp_ReportDettaglioOrdini
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportDettaglioOrdini (
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
    @NascondiOrdiniRinnovati BIT = 0
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
        C.TipoCliente,
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
        D.PKDataCompetenza

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
        C.TipoCliente,
        D.TipoFatturazione,
        D.NoteIntestazione,
        C.Email,
        MT.MacroTipologia,
        ICA.ProvvigioneNuovo,
        ICA.ProvvigioneRinnovo,
        CASE WHEN D.NoteDecisionali LIKE @AgenteProprietarioPrefix + N'%)' THEN SUBSTRING(D.NoteDecisionali, LEN(@AgenteProprietarioPrefix)+1, LEN(D.NoteDecisionali) - LEN(@AgenteProprietarioPrefix) - 1) ELSE GA.CapoArea END,
        COALESCE(CASE WHEN DATEDIFF(MONTH, D.PKDataInizioContratto, D.PKDataFineContratto) >= 24 THEN LPTP.LiquidazioneProvvigioneTeorica ELSE LPTA.LiquidazioneProvvigioneTeorica END, N''),
        D.IDDocumentoRinnovato,
        D.PKDataCompetenza
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
        ROW_NUMBER() OVER (PARTITION BY O.PrefissoCapoArea ORDER BY O.CodiceCliente, O.NumeroDocumento) AS rnCapoArea,
        O.TipoCliente,
        O.TipoFatturazione,
        O.PKDataFattura,
        O.NoteIntestazione,
        O.Email,
        O.ProvvigioneTeorica,
        O.LiquidazioneProvvigioneTeorica,
        O.AgenteProprietario,
        O.IDDocumentoRinnovato,
        O.PKDataCompetenza

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
    DATEDIFF(MONTH, DOR.PKDataInizioContratto, DATEADD(DAY, 1, DOR.PKDataFineContratto)) AS DurataMesiRinnovo

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

/*

SELECT
    PKData AS PKDataInizioPeriodo,
    AnnoMese_IT AS DataInizioPeriodo

FROM Dim.Data
WHERE PKData BETWEEN CAST('20200101' AS DATE) AND CAST(CURRENT_TIMESTAMP AS DATE)
    AND DATEPART(DAY, PKData) = 1
ORDER BY PKData DESC;
GO

SELECT
    DATEADD(DAY, -1, DATEADD(MONTH, 1, PKData)) AS PKDataFinePeriodo,
    AnnoMese_IT AS DataFinePeriodo

FROM Dim.Data
WHERE PKData BETWEEN CAST('20200101' AS DATE) AND CAST(CURRENT_TIMESTAMP AS DATE)
    AND DATEPART(DAY, PKData) = 1
ORDER BY PKData DESC;
GO

SELECT DISTINCT
    GA.GruppoAgenti,
    GA.GruppoAgenti AS GruppoAgentiDescrizione

FROM Fact.Documenti D
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
    AND GA.GruppoAgenti <> N''
WHERE D.PKDataCompetenza >= CAST('20200101' AS DATE)

UNION ALL SELECT NULL, N'Tutti'

ORDER BY GA.GruppoAgenti;
GO

SELECT DISTINCT
    GA.CapoArea,
    GA.CapoArea AS CapoAreaDescrizione

FROM Fact.Documenti D
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
    AND GA.CapoArea <> N''
WHERE D.PKDataCompetenza >= CAST('20200101' AS DATE)

UNION ALL SELECT NULL, N'Tutti'

ORDER BY GA.CapoArea;
GO

SELECT
    'M' AS TipoFiltroData,
    N'Mese' AS TipoFiltroDataDescrizione

UNION ALL SELECT 'P', N'Periodo'

ORDER BY TipoFiltroData;
GO

WITH TipoDataDetail
AS (
    SELECT
        'C' AS TipoData,
        N'Competenza (data ordine)' AS TipoDataDescrizione,
        1 AS Sorting

    UNION ALL SELECT 'I', N'Inizio contratto', 2
    UNION ALL SELECT 'F', N'Fine contratto', 3
)
SELECT
    TDD.TipoData,
    TDD.TipoDataDescrizione

FROM TipoDataDetail TDD
ORDER BY TDD.Sorting;
GO

SELECT DISTINCT
    Libero1 AS Azione,
    Libero1 AS AzioneDescrizione

FROM Fact.Documenti

UNION ALL SELECT NULL, N'Tutte'

ORDER BY Azione;
GO

*/

/**
 * @storedprocedure Fact.usp_ReportDettaglioOrdiniInScadenza
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportDettaglioOrdiniInScadenza (
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

GRANT EXECUTE ON Fact.usp_ReportDettaglioOrdiniInScadenza TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE;
DECLARE @PKDataFinePeriodo DATE;
DECLARE @GruppoAgenti NVARCHAR(60);
DECLARE @CapoArea NVARCHAR(60);

EXEC Fact.usp_ReportDettaglioOrdiniInScadenza
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @PKDataFinePeriodo = @PKDataFinePeriodo,
    @GruppoAgenti = @GruppoAgenti,
    @CapoArea = @CapoArea;
GO

/**
 * @storedprocedure Fact.usp_ReportDettaglioRigheOrdine
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportDettaglioRigheOrdine (
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
        C.TipoCliente,
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
        C.TipoCliente,
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

GRANT EXECUTE ON Fact.usp_ReportDettaglioRigheOrdine TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE = '20211201';
DECLARE @PKDataFinePeriodo DATE;
DECLARE @GruppoAgenti NVARCHAR(60);
DECLARE @CapoArea NVARCHAR(60);

EXEC Fact.usp_ReportDettaglioRigheOrdine
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @PKDataFinePeriodo = @PKDataFinePeriodo,
    @GruppoAgenti = @GruppoAgenti,
    @CapoArea = @CapoArea;
GO

/**
 * @storedprocedure Fact.usp_ReportDettaglioFatture
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportDettaglioFatture (
    @PKDataInizioPeriodo DATE,
    @PKDataFinePeriodo DATE,
    @GruppoAgenti NVARCHAR(60),
    @CapoArea NVARCHAR(60),
    --@TipoFiltroData CHAR(1) = 'M', -- 'M': mese, 'P': periodo
    @TipoData CHAR(1) = 'C', -- 'C': competenza (data ordine), 'I': inizio contratto, 'F': fine contratto
    @RagioneSociale NVARCHAR(120) = NULL,
    @CodiceCliente NVARCHAR(10) = NULL,
    @PartitaIVA NVARCHAR(20) = NULL,
    @TipoReport CHAR(1) = NULL -- 'F': Formazione, 'M': My Solution
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
        C.TipoCliente,
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
        C.TipoCliente,
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

GRANT EXECUTE ON Fact.usp_ReportDettaglioFatture TO cesidw_reader;
GO

DECLARE @PKDataInizioPeriodo DATE = '20230101';
DECLARE @PKDataFinePeriodo DATE = '20230331';
DECLARE @GruppoAgenti NVARCHAR(60);
DECLARE @CapoArea NVARCHAR(60);

EXEC Fact.usp_ReportDettaglioFatture
    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
    @PKDataFinePeriodo = @PKDataFinePeriodo,
    @GruppoAgenti = @GruppoAgenti,
    @CapoArea = @CapoArea,
    @TipoReport = 'M';
GO

/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazione
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazione (
    @CodiceEsercizio CHAR(4),
    @CapoArea NVARCHAR(60),
    @Agente NVARCHAR(60)
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@CodiceEsercizio IS NULL)
    BEGIN

        SELECT @CodiceEsercizio = CAST(YEAR(CURRENT_TIMESTAMP) AS CHAR(4));

    END;

    SELECT
        C.Regione,
        C.Provincia,
        DR.Mese,
        DR.Mese_IT AS MeseDescrizione,
        SUM(D.ImportoTotale) AS ImportoTotale

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
        AND (
            @CapoArea IS NULL
            OR C.CapoAreaDefault = @CapoArea
        )
        AND (
            @Agente IS NULL
            OR C.AgenteDefault = @Agente
        )
    WHERE D.CodiceEsercizio = @CodiceEsercizio
        AND D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.Regione,
        C.Provincia,
        DR.Mese,
        DR.Mese_IT
    ORDER BY C.Regione,
        C.Provincia,
        DR.Mese;

END;
GO

SELECT
    D.Profilo,
    COUNT(1)
FROM Fact.Documenti D
WHERE D.IsProfiloValidoPerStatisticaFatturato = CAST(1 AS BIT)
GROUP BY D.Profilo

GRANT EXECUTE ON Fact.usp_ReportFatturatoFormazione TO cesidw_reader;
GO

DECLARE @CodiceEsercizio CHAR(4) = NULL;
DECLARE @CapoArea NVARCHAR(60) = NULL;
DECLARE @Agente NVARCHAR(60) = NULL;

EXEC Fact.usp_ReportFatturatoFormazione
    @CodiceEsercizio = @CodiceEsercizio,
    @CapoArea = @CapoArea,
    @Agente = @Agente;

EXEC Fact.usp_ReportFatturatoFormazione
    @CodiceEsercizio = @CodiceEsercizio,
    @CapoArea = N'TUROLLA PAOLA',
    @Agente = NULL;

EXEC Fact.usp_ReportFatturatoFormazione
    @CodiceEsercizio = @CodiceEsercizio,
    @CapoArea = NULL,
    @Agente = N'TUROLLA';
GO

/*

SELECT DISTINCT
    CodiceEsercizio
FROM Fact.Documenti
WHERE IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
ORDER BY CodiceEsercizio DESC;
GO

SELECT DISTINCT
    CapoAreaDefault,
    CapoAreaDefault AS CapoAreaDefaultDescrizione

FROM Dim.Cliente
WHERE CapoAreaDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'

ORDER BY CapoAreaDefault;
GO

SELECT DISTINCT
    AgenteDefault,
    AgenteDefault AS AgenteDefaultDescrizione

FROM Dim.Cliente
WHERE AgenteDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'

ORDER BY AgenteDefault;
GO

SELECT DISTINCT
    C.CapoAreaDefault,
    C.CapoAreaDefault AS CapoAreaDefaultDescrizione

FROM Dim.Cliente C
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = C.CapoAreaDefault
    AND AUCA.ADUser = @ADUser

WHERE C.CapoAreaDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY CapoAreaDefault;
GO

SELECT DISTINCT
    C.AgenteDefault,
    C.AgenteDefault AS AgenteDefaultDescrizione

FROM Dim.Cliente C
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = C.CapoAreaDefault
    AND AUCA.ADUser = @ADUser
WHERE C.AgenteDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY AgenteDefault;
GO

*/

/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneDettaglio
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglio (
    @AnnoCorrente INT = NULL,
    @CapoAreaDefault NVARCHAR(60) = NULL,
    @AgenteDefault NVARCHAR(40) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @CodiceEsercizioMasterCorrente NVARCHAR(10),
            @CodiceEsercizioMasterPrecedente NVARCHAR(10);

    SELECT @AnnoCorrente = YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP));

    SELECT @CodiceEsercizioMasterCorrente = CONVERT(NVARCHAR(4), @AnnoCorrente) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente + 1),
        @CodiceEsercizioMasterPrecedente = CONVERT(NVARCHAR(4), @AnnoCorrente - 1) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente);

    WITH IscrizioniMaster
    AS (
        SELECT
            D.PKCliente,
            ACM.CategoriaMaster,
            ACM.CodiceEsercizioMaster AS CodiceEsercizio,
            MAX(D.PKDataDocumento) AS DataUltimaFattura,
            COUNT(1) AS NumeroIscritti,
            SUM(D.ImportoTotale * ACM.Percentuale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
            AND (
                @CapoAreaDefault IS NULL
                OR C.CapoAreaDefault = @CapoAreaDefault
            )
            AND (
                @AgenteDefault IS NULL
                OR C.AgenteDefault = @AgenteDefault
            )
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
            AND A.CodiceEsercizioMaster IN (@CodiceEsercizioMasterCorrente, @CodiceEsercizioMasterPrecedente)
        INNER JOIN Staging.ArticoloCategoriaMaster ACM ON ACM.id_articolo = A.id_articolo
        WHERE D.IDProfilo = N'ORDSEM'
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY D.PKCliente,
            ACM.CategoriaMaster,
            ACM.CodiceEsercizioMaster
    ),
    ClientiMaster
    AS (
        SELECT DISTINCT
            IM.PKCliente
        FROM IscrizioniMaster IM
    )
    SELECT
        C.PKCliente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.IsAbbonato,
        C.CapoAreaDefault,
        C.AgenteDefault,
        C.Email,
        C.Telefono,
        C.Cellulare,
        @CodiceEsercizioMasterPrecedente AS CodiceEsercizioPrecedente,
        @CodiceEsercizioMasterCorrente AS CodiceEsercizioCorrente,
        COALESCE(CONVERT(NVARCHAR(10), IMAP.DataUltimaFattura, 103), N'') AS DataUltimaFatturaEsercizioPrecedente,
        IMAP.NumeroIscritti AS NumeroIscrittiAnnoPrecedente,
        IMAP.ImportoTotale AS ImportoTotaleAnnoPrecedente,
        COALESCE(CONVERT(NVARCHAR(10), IMAC.DataUltimaFattura, 103), N'') AS DataUltimaFatturaEsercizioCorrente,
        IMAC.NumeroIscritti AS NumeroIscrittiAnnoCorrente,
        IMAC.ImportoTotale AS ImportoTotaleAnnoCorrente,
        COALESCE(CONVERT(NVARCHAR(10), IMMAP.DataUltimaFattura, 103), N'') AS DataUltimaFatturaMiniMasterEsercizioPrecedente,
        IMMAP.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoPrecedente,
        IMMAP.ImportoTotale AS ImportoTotaleMiniMasterAnnoPrecedente,
        COALESCE(CONVERT(NVARCHAR(10), IMMAC.DataUltimaFattura, 103), N'') AS DataUltimaFatturaMiniMasterEsercizioCorrente,
        IMMAC.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoCorrente,
        IMMAC.ImportoTotale AS ImportoTotaleMiniMasterAnnoCorrente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMAP.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMAP.DataUltimaFattura), N'') AS MeseUltimaFatturaEsercizioPrecedente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMAC.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMAC.DataUltimaFattura), N'') AS MeseUltimaFatturaEsercizioCorrente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMMAP.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMMAP.DataUltimaFattura), N'') AS MeseUltimaFatturaMiniMasterEsercizioPrecedente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMMAC.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMMAC.DataUltimaFattura), N'') AS MeseUltimaFatturaMiniMasterEsercizioCorrente,
        C.Provincia

    FROM ClientiMaster CM
    INNER JOIN Dim.Cliente C ON C.PKCliente = CM.PKCliente
    LEFT JOIN IscrizioniMaster IMAC ON IMAC.PKCliente = CM.PKCliente
        AND IMAC.CategoriaMaster = N'Master MySolution'
        AND IMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    LEFT JOIN IscrizioniMaster IMAP ON IMAP.PKCliente = CM.PKCliente
        AND IMAP.CategoriaMaster = N'Master MySolution'
        AND IMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente
    LEFT JOIN IscrizioniMaster IMMAC ON IMMAC.PKCliente = CM.PKCliente
        AND IMMAC.CategoriaMaster = N'Mini Master Revisione'
        AND IMMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    LEFT JOIN IscrizioniMaster IMMAP ON IMMAP.PKCliente = CM.PKCliente
        AND IMMAP.CategoriaMaster = N'Mini Master Revisione'
        AND IMMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente

    ORDER BY C.CodiceCliente;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoFormazioneDettaglio TO cesidw_reader;
GO

DECLARE @AnnoCorrente INT = 2024;
DECLARE @CapoAreaDefault NVARCHAR(60) = NULL;
DECLARE @AgenteDefault NVARCHAR(40) = NULL;

EXEC Fact.usp_ReportFatturatoFormazioneDettaglio @AnnoCorrente = @AnnoCorrente,
                                     @CapoAreaDefault = @CapoAreaDefault,
                                     @AgenteDefault = @AgenteDefault;

EXEC Fact.usp_ReportFatturatoFormazioneDettaglio @AnnoCorrente = @AnnoCorrente,
                                     @CapoAreaDefault = N'TUROLLA PAOLA',
                                     @AgenteDefault = NULL;

EXEC Fact.usp_ReportFatturatoFormazioneDettaglio @AnnoCorrente = @AnnoCorrente,
                                     @CapoAreaDefault = NULL,
                                     @AgenteDefault = N'TUROLLA';
GO

/*

SELECT DISTINCT
    CapoAreaDefault,
    CapoAreaDefault AS CapoAreaDefaultDescrizione

FROM Dim.Cliente
WHERE CapoAreaDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'

ORDER BY CapoAreaDefault;
GO

SELECT DISTINCT
    AgenteDefault,
    AgenteDefault AS AgenteDefaultDescrizione

FROM Dim.Cliente
WHERE AgenteDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'

ORDER BY AgenteDefault;
GO

SELECT DISTINCT
    C.CapoAreaDefault,
    C.CapoAreaDefault AS CapoAreaDefaultDescrizione

FROM Dim.Cliente C
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = C.CapoAreaDefault
    AND AUCA.ADUser = @ADUser

WHERE C.CapoAreaDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY CapoAreaDefault;
GO

SELECT DISTINCT
    C.AgenteDefault,
    C.AgenteDefault AS AgenteDefaultDescrizione

FROM Dim.Cliente C
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = C.CapoAreaDefault
    AND AUCA.ADUser = @ADUser
WHERE C.AgenteDefault <> N''

UNION ALL

SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY AgenteDefault;
GO

*/

/**
 * @storedprocedure Fact.usp_ReportFatturatoGenerale
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoGenerale (
    @CodiceEsercizio CHAR(4),
    @CapoArea NVARCHAR(60),
    @IsProfiloValidoPerStatisticaFatturatoFormazione BIT
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@CodiceEsercizio IS NULL)
    BEGIN

        SELECT @CodiceEsercizio = CAST(YEAR(CURRENT_TIMESTAMP) AS CHAR(4));

    END;

    SELECT
        GA.CapoArea,
        D.Libero2 AS Tipologia,
        DR.Mese,
        DR.Mese_IT AS MeseDescrizione,
        SUM(D.ImportoTotale) AS ImportoTotale

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
    WHERE D.CodiceEsercizio = @CodiceEsercizio
        AND D.IsProfiloValidoPerStatisticaFatturato = CAST(1 AS BIT)
        AND (
            @IsProfiloValidoPerStatisticaFatturatoFormazione IS NULL
            OR D.IsProfiloValidoPerStatisticaFatturatoFormazione = @IsProfiloValidoPerStatisticaFatturatoFormazione
        )
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY GA.CapoArea,
        D.Libero2,
        DR.Mese,
        DR.Mese_IT
    ORDER BY GA.CapoArea,
        D.Libero2,
        DR.Mese;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoGenerale TO cesidw_reader;
GO

DECLARE @CodiceEsercizio CHAR(4) = NULL;
DECLARE @CapoArea NVARCHAR(60) = NULL;
DECLARE @IsProfiloValidoPerStatisticaFatturatoFormazione BIT = NULL;

EXEC Fact.usp_ReportFatturatoGenerale
    @CodiceEsercizio = @CodiceEsercizio,
    @CapoArea = @CapoArea,
    @IsProfiloValidoPerStatisticaFatturatoFormazione = @IsProfiloValidoPerStatisticaFatturatoFormazione;
GO

/*

SELECT DISTINCT
    CodiceEsercizio
FROM Fact.Documenti
WHERE IsProfiloValidoPerStatisticaFatturato = CAST(1 AS BIT)
ORDER BY CodiceEsercizio DESC;
GO

SELECT DISTINCT
    GA.CapoArea,
    GA.CapoArea AS CapoAreaDescrizione

FROM Fact.Documenti D
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
    AND GA.CapoArea <> N''
WHERE D.IsProfiloValidoPerStatisticaFatturato = CAST(1 AS BIT)

UNION ALL SELECT NULL, N'Tutti'

ORDER BY CapoArea;
GO

SELECT
    CAST(0 AS BIT) AS IsProfiloValidoPerStatisticaFatturatoFormazione,
    N'Escluso Formazione' AS IsProfiloValidoPerStatisticaFatturatoFormazioneDescrizione

UNION ALL SELECT 1, N'Solo Formazione'
UNION ALL SELECT NULL, N'Tutto'

ORDER BY IsProfiloValidoPerStatisticaFatturatoFormazione;
GO

*/

/**
 * @storedprocedure Fact.usp_ReportOrdinatoGenerale
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportOrdinatoGenerale (
    @AnnoCorrente INT,
    @CapoArea NVARCHAR(60) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @AnnoPrecedente INT = @AnnoCorrente - 1;

    DECLARE @CodiceEsercizioCorrente CHAR(4) = CONVERT(CHAR(4), @AnnoCorrente);
    DECLARE @CodiceEsercizioPrecedente CHAR(4) = CONVERT(CHAR(4), @AnnoPrecedente);

    --SELECT @AnnoCorrente, @AnnoPrecedente, @CodiceEsercizioCorrente, @CodiceEsercizioPrecedente;

    WITH DataDetail
    AS (
        SELECT
            GAR.CapoArea,
            D.Libero2 AS Tipologia,
            CASE MT.MacroTipologia
              WHEN N'Nuova vendita' THEN N'nuove vendite'
              WHEN N'Rinnovo' THEN N'rinnovi'
              ELSE MT.MacroTipologia
            END AS MacroTipologia,
            DR.Mese,
            DR.Mese_IT,
            D.CodiceEsercizio,
            SUM(D.Quote) AS QuoteTotali,
            SUM(D.ImportoTotale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia

        WHERE D.CodiceEsercizio IN (@CodiceEsercizioCorrente, @CodiceEsercizioPrecedente)
            AND D.Profilo = N'ORDINE CLIENTE'
            --AND D.TipoSoggettoCommerciale = N'C'
            AND D.Registro = N'ORDINI VENDITE'
            --AND D.Libero2 IN (
            --    N'RECUPERO',
            --    N'RINNOVO AGENTE',
            --    N'RINNOVO AUTOMATICO',
            --    N'RINNOVO CONCORDATO',
            --    N'RINNOVO DIREZIONALI'
            --)
            AND D.IsDeleted = CAST(0 AS BIT)
   
        GROUP BY GAR.CapoArea,
            D.Libero2,
            MT.MacroTipologia,
            DR.Mese,
            DR.Mese_IT,
            D.CodiceEsercizio
    ),
    CapiAreaTipologie
    AS (
        SELECT DISTINCT
            DD.CapoArea,
            DD.Tipologia,
            DD.MacroTipologia
        FROM DataDetail DD
    ),
    CodiciEsercizio
    AS (
        SELECT @CodiceEsercizioCorrente AS CodiceEsercizio
        UNION ALL SELECT @CodiceEsercizioPrecedente
    ),
    Months
    AS (
        SELECT DISTINCT
            D.Mese,
            D.Mese_IT

        FROM Dim.Data D
        WHERE D.Anno IN (@AnnoCorrente, @AnnoPrecedente)
    ),
    DataRecap
    AS (
        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.Tipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea
    )
    SELECT
        T.CapoArea,
        T.Tipologia,
        T.Mese,
        T.Mese_IT,
        T.QuoteTotaliEsercizioCorrente,
        T.QuoteTotaliEsercizioPrecedente,
        T.ImportoTotaleEsercizioCorrente,
        T.ImportoTotaleEsercizioPrecedente,
        T.DeltaVsEsercizioPrecedente,
        T.rnCapoArea

    FROM (
        SELECT
            DR.CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            DR.QuoteTotaliEsercizioCorrente,
            DR.QuoteTotaliEsercizioPrecedente,
            DR.ImportoTotaleEsercizioCorrente,
            DR.ImportoTotaleEsercizioPrecedente,

            CASE WHEN COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0) = 0.0 THEN NULL ELSE DR.ImportoTotaleEsercizioCorrente / DR.ImportoTotaleEsercizioPrecedente - 1.0 END AS DeltaVsEsercizioPrecedente,
            DENSE_RANK() OVER (ORDER BY DR.CapoArea) AS rnCapoArea

        FROM DataRecap DR

        UNION ALL

        SELECT
            N'Totale' AS CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            SUM(DR.QuoteTotaliEsercizioCorrente) AS QuoteTotaliEsercizioCorrente,
            SUM(DR.QuoteTotaliEsercizioPrecedente) AS QuoteTotaliEsercizioPrecedente,
            SUM(DR.ImportoTotaleEsercizioCorrente) AS ImportoTotaleEsercizioCorrente,
            SUM(DR.ImportoTotaleEsercizioPrecedente) AS ImportoTotaleEsercizioPrecedente,
            CASE WHEN SUM(COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0)) = 0.0 THEN NULL ELSE SUM(DR.ImportoTotaleEsercizioCorrente) / SUM(DR.ImportoTotaleEsercizioPrecedente) - 1.0 END AS DeltaVsEsercizioPrecedente,
            999 AS rnCapoArea

        FROM DataRecap DR
        GROUP BY DR.Tipologia,
            DR.Mese,
            DR.Mese_IT

    ) T
    ORDER BY T.rnCapoArea,
        T.Tipologia,
        T.Mese;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportOrdinatoGenerale TO cesidw_reader;
GO

DECLARE @AnnoCorrente INT = 2022;
DECLARE @CapoArea NVARCHAR(60) = NULL;

EXEC Fact.usp_ReportOrdinatoGenerale @AnnoCorrente = @AnnoCorrente,
                                     @CapoArea = @CapoArea;
GO

/*

SELECT DISTINCT
    D.Anno
FROM Dim.Data D
WHERE D.Anno > 1900
    AND D.Anno <= YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP))
ORDER BY D.Anno DESC;

SELECT DISTINCT
    GAR.CapoArea,
    GAR.CapoArea AS CapoAreaDescrizione

FROM Fact.Documenti D
INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = GAR.CapoArea
    AND AUCA.ADUser = @ADUser
WHERE D.CodiceEsercizio IN (
    CONVERT(CHAR(4), @AnnoCorrente),
    CONVERT(CHAR(4), @AnnoCorrente - 1)
)

UNION ALL SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY CapoArea;

*/

/**
 * @storedprocedure Fact.usp_ReportFatturatoGeneraleNew
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoGeneraleNew (
    @AnnoCorrente INT,
    @CapoArea NVARCHAR(60) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @AnnoPrecedente INT = @AnnoCorrente - 1;

    DECLARE @CodiceEsercizioCorrente CHAR(4) = CONVERT(CHAR(4), @AnnoCorrente);
    DECLARE @CodiceEsercizioPrecedente CHAR(4) = CONVERT(CHAR(4), @AnnoPrecedente);

    --SELECT @AnnoCorrente, @AnnoPrecedente, @CodiceEsercizioCorrente, @CodiceEsercizioPrecedente;

    WITH DataDetail
    AS (
        SELECT
            GAR.CapoArea,
            D.Libero2 AS Tipologia,
            CASE MT.MacroTipologia
              WHEN N'Nuova vendita' THEN N'nuove vendite'
              WHEN N'Rinnovo' THEN N'rinnovi'
              ELSE MT.MacroTipologia
            END AS MacroTipologia,
            DR.Mese,
            DR.Mese_IT,
            D.CodiceEsercizio,
            SUM(D.Quote) AS QuoteTotali,
            SUM(D.ImportoTotale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia
        WHERE D.CodiceEsercizio IN (@CodiceEsercizioCorrente, @CodiceEsercizioPrecedente)
            AND D.IsProfiloValidoPerStatisticaFatturato = CAST(1 AS BIT)
            AND D.IsDeleted = CAST(0 AS BIT)
   
        GROUP BY GAR.CapoArea,
            D.Libero2,
            MT.MacroTipologia,
            DR.Mese,
            DR.Mese_IT,
            D.CodiceEsercizio
    ),
    CapiAreaTipologie
    AS (
        SELECT DISTINCT
            DD.CapoArea,
            DD.Tipologia,
            DD.MacroTipologia
        FROM DataDetail DD
    ),
    CodiciEsercizio
    AS (
        SELECT @CodiceEsercizioCorrente AS CodiceEsercizio
        UNION ALL SELECT @CodiceEsercizioPrecedente
    ),
    Months
    AS (
        SELECT DISTINCT
            D.Mese,
            D.Mese_IT

        FROM Dim.Data D
        WHERE D.Anno IN (@AnnoCorrente, @AnnoPrecedente)
    ),
    DataRecap
    AS (
        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.Tipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea
    )
    SELECT
        T.CapoArea,
        T.Tipologia,
        T.Mese,
        T.Mese_IT,
        T.QuoteTotaliEsercizioCorrente,
        T.QuoteTotaliEsercizioPrecedente,
        T.ImportoTotaleEsercizioCorrente,
        T.ImportoTotaleEsercizioPrecedente,
        T.DeltaVsEsercizioPrecedente,
        T.rnCapoArea

    FROM (
        SELECT
            DR.CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            DR.QuoteTotaliEsercizioCorrente,
            DR.QuoteTotaliEsercizioPrecedente,
            DR.ImportoTotaleEsercizioCorrente,
            DR.ImportoTotaleEsercizioPrecedente,

            CASE WHEN COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0) = 0.0 THEN NULL ELSE DR.ImportoTotaleEsercizioCorrente / DR.ImportoTotaleEsercizioPrecedente - 1.0 END AS DeltaVsEsercizioPrecedente,
            DENSE_RANK() OVER (ORDER BY DR.CapoArea) AS rnCapoArea

        FROM DataRecap DR

        UNION ALL

        SELECT
            N'Totale' AS CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            SUM(DR.QuoteTotaliEsercizioCorrente) AS QuoteTotaliEsercizioCorrente,
            SUM(DR.QuoteTotaliEsercizioPrecedente) AS QuoteTotaliEsercizioPrecedente,
            SUM(DR.ImportoTotaleEsercizioCorrente) AS ImportoTotaleEsercizioCorrente,
            SUM(DR.ImportoTotaleEsercizioPrecedente) AS ImportoTotaleEsercizioPrecedente,
            CASE WHEN SUM(COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0)) = 0.0 THEN NULL ELSE SUM(DR.ImportoTotaleEsercizioCorrente) / SUM(DR.ImportoTotaleEsercizioPrecedente) - 1.0 END AS DeltaVsEsercizioPrecedente,
            999 AS rnCapoArea

        FROM DataRecap DR
        GROUP BY DR.Tipologia,
            DR.Mese,
            DR.Mese_IT

    ) T
    ORDER BY T.rnCapoArea,
        T.Tipologia,
        T.Mese;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoGeneraleNew TO cesidw_reader;
GO

DECLARE @AnnoCorrente INT = 2022;
DECLARE @CapoArea NVARCHAR(60) = NULL;

EXEC Fact.usp_ReportFatturatoGeneraleNew @AnnoCorrente = @AnnoCorrente,
                                     @CapoArea = @CapoArea;
GO

/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio (
    @Anno INT = NULL,
    @CapoArea NVARCHAR(60) = NULL,
    @IDCategoria INT = 0
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@Anno IS NULL) SET @Anno = YEAR(CURRENT_TIMESTAMP);

    WITH OrdinamentoCorsi
    AS (
        SELECT
            A.PKArticolo,
            SUM(D.ImportoTotale) AS ImportoTotale,
            ROW_NUMBER() OVER (ORDER BY SUM(D.ImportoTotale)) AS rn

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
            AND DC.Anno = @Anno
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY A.PKArticolo
    )
    SELECT
        A.PKArticolo,
        A.Codice AS CodiceCorso,
        CASE @IDCategoria
          WHEN 0 THEN A.CategoriaMaster
          WHEN 1 THEN A.Data1
          WHEN 2 THEN A.Data2
          WHEN 3 THEN A.Data3
          WHEN 4 THEN A.Data4
          WHEN 5 THEN A.Data5
          WHEN 6 THEN A.Data6
          ELSE A.CategoriaMaster
        END AS CategoriaMaster,
        A.Descrizione AS DescrizioneCorso,
        DC.Mese,
        DC.Mese_IT,
        COUNT(1) AS Quantita,
        SUM(D.ImportoTotale) AS ImportoTotale,
        COALESCE(OC.rn, 0) AS rn

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND DC.Anno = @Anno
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
        AND (
            @CapoArea IS NULL
            OR GAR.CapoArea = @CapoArea
        )
    LEFT JOIN OrdinamentoCorsi OC ON OC.PKArticolo = D.PKArticolo
    WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY A.PKArticolo,
        A.Codice,
        CASE @IDCategoria
          WHEN 0 THEN A.CategoriaMaster
          WHEN 1 THEN A.Data1
          WHEN 2 THEN A.Data2
          WHEN 3 THEN A.Data3
          WHEN 4 THEN A.Data4
          WHEN 5 THEN A.Data5
          WHEN 6 THEN A.Data6
          ELSE A.CategoriaMaster
        END,
        A.Descrizione,
        DC.Mese,
        DC.Mese_IT,
        OC.rn
    ORDER BY A.Codice,
        DC.Mese;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio TO cesidw_reader;
GO

EXEC Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio @Anno = NULL, -- int
                                                          @CapoArea = NULL, -- nvarchar(60)
                                                          @IDCategoria = 1;
GO

/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneDettaglioCorsi
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglioCorsi (
    @DataInizio DATE = NULL,
    @DataFine DATE = NULL,
    @CapoArea NVARCHAR(60) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@DataInizio IS NULL) SELECT @DataInizio = DATEADD(DAY, -7, CONVERT(DATE, CURRENT_TIMESTAMP));
    IF (@DataFine IS NULL) SELECT @DataFine = DATEADD(DAY, 6, @DataInizio);

    --SELECT @DataInizio, @DataFine;

    WITH OrdinamentoCorsi
    AS (
        SELECT
            A.PKArticolo,
            SUM(D.ImportoTotale) AS ImportoTotale,
            ROW_NUMBER() OVER (ORDER BY SUM(D.ImportoTotale)) AS rn

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
            AND DC.PKData BETWEEN @DataInizio AND @DataFine
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY A.PKArticolo
    )
    SELECT
        @DataInizio AS DataInizio,
        @DataFine AS DataFine,
        A.PKArticolo,
        A.Codice AS CodiceCorso,
        A.CategoriaMaster,
        A.Descrizione AS DescrizioneCorso,
        CASE C.IsAbbonato WHEN CAST(1 AS BIT) THEN N'Abbonati' WHEN CAST(0 AS BIT) THEN N'Non abbonati' END AS IsAbbonato,
        COUNT(1) AS Quantita,
        SUM(D.ImportoTotale) AS ImportoTotale,
        COALESCE(OC.rn, 0) AS rn

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND DC.PKData BETWEEN @DataInizio AND @DataFine
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
        AND (
            @CapoArea IS NULL
            OR GAR.CapoArea = @CapoArea
        )
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
    LEFT JOIN OrdinamentoCorsi OC ON OC.PKArticolo = D.PKArticolo
    WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY A.PKArticolo,
        A.Codice,
        A.CategoriaMaster,
        A.Descrizione,
        CASE C.IsAbbonato WHEN CAST(1 AS BIT) THEN N'Abbonati' WHEN CAST(0 AS BIT) THEN N'Non abbonati' END,
        OC.rn
    ORDER BY A.Codice,
        IsAbbonato;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoFormazioneDettaglioCorsi TO cesidw_reader;
GO

EXEC Fact.usp_ReportFatturatoFormazioneDettaglioCorsi @DataInizio = NULL, -- date
                                                      @DataFine = NULL, -- date
                                                      @CapoArea = NULL  -- nvarchar(60)
GO

/**
 * @storedprocedure Fact.usp_ReportOrdinatoGeneraleBudget
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportOrdinatoGeneraleBudget (
    @AnnoCorrente INT,
    @CapoArea NVARCHAR(60) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @AnnoPrecedente INT = @AnnoCorrente - 1;

    DECLARE @CodiceEsercizioCorrente CHAR(4) = CONVERT(CHAR(4), @AnnoCorrente);
    DECLARE @CodiceEsercizioPrecedente CHAR(4) = CONVERT(CHAR(4), @AnnoPrecedente);

    --SELECT @AnnoCorrente, @AnnoPrecedente, @CodiceEsercizioCorrente, @CodiceEsercizioPrecedente;

    WITH DataDetail
    AS (
        SELECT
            GAR.CapoArea,
            D.Libero2 AS Tipologia,
            CASE MT.MacroTipologia
              WHEN N'Nuova vendita' THEN N'nuove vendite'
              WHEN N'Rinnovo' THEN N'rinnovi'
              ELSE MT.MacroTipologia
            END AS MacroTipologia,
            DR.Mese,
            DR.Mese_IT,
            D.CodiceEsercizio,
            SUM(D.Quote) AS QuoteTotali,
            SUM(D.ImportoTotale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
            --AND DR.IsOrdinazioneChiusa = CAST(1 AS BIT)
            AND DR.Mese <= MONTH(CURRENT_TIMESTAMP)
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        INNER JOIN Dim.MacroTipologia MT ON MT.PKMacroTipologia = D.PKMacroTipologia

        WHERE D.CodiceEsercizio = @CodiceEsercizioCorrente
            AND D.Profilo = N'ORDINE CLIENTE'
            --AND D.TipoSoggettoCommerciale = N'C'
            AND D.Registro = N'ORDINI VENDITE'
            --AND D.Libero2 IN (
            --    N'RECUPERO',
            --    N'RINNOVO AGENTE',
            --    N'RINNOVO AUTOMATICO',
            --    N'RINNOVO CONCORDATO',
            --    N'RINNOVO DIREZIONALI'
            --)
            AND D.IsDeleted = CAST(0 AS BIT)

            AND MT.MacroTipologia = N'Nuova vendita'

            GROUP BY CASE MT.MacroTipologia
                  WHEN N'Nuova vendita' THEN N'nuove vendite'
                  WHEN N'Rinnovo' THEN N'rinnovi'
                  ELSE MT.MacroTipologia
                END,
                GAR.CapoArea,
                D.Libero2,
                DR.Mese,
                DR.Mese_IT,
                D.CodiceEsercizio

        UNION ALL

        SELECT
            CA.CapoArea,
            N'NUOVO' AS Tipologia,
            N'nuove vendite' AS MacroTipologia,
            DIM.Mese,
            DIM.Mese_IT,
            @CodiceEsercizioPrecedente AS CodiceEsercizio,
            NULL AS QuoteTotali,
            SUM(B.ImportoBudgetNuoveVendite) AS ImportoTotale

        FROM Fact.Budget B
        INNER JOIN Dim.Data DIM ON DIM.PKData = B.PKData
            AND DIM.Anno = @AnnoCorrente
            --AND DIM.IsOrdinazioneChiusa = CAST(1 AS BIT)
            AND DIM.Mese <= MONTH(CURRENT_TIMESTAMP)
        INNER JOIN Dim.CapoArea CA ON CA.PKCapoArea = B.PKCapoArea
            AND (
                @CapoArea IS NULL
                OR CA.CapoArea = @CapoArea
            )
        GROUP BY CA.CapoArea,
            DIM.Mese,
            DIM.Mese_IT
    ),
    CapiAreaTipologie
    AS (
        SELECT DISTINCT
            DD.CapoArea,
            DD.Tipologia,
            DD.MacroTipologia
        FROM DataDetail DD
    ),
    CodiciEsercizio
    AS (
        SELECT @CodiceEsercizioCorrente AS CodiceEsercizio
        UNION ALL SELECT @CodiceEsercizioPrecedente
    ),
    Months
    AS (
        SELECT DISTINCT
            D.Mese,
            D.Mese_IT

        FROM Dim.Data D
        WHERE D.Anno IN (@AnnoCorrente, @AnnoPrecedente)
            --AND D.IsOrdinazioneMensileChiusa = CAST(1 AS BIT)
            AND D.Mese <= MONTH(CURRENT_TIMESTAMP)
    ),
    DataRecap
    AS (
        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.Tipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            CAT.Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.Tipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale  ' + CAT.MacroTipologia AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea,
            CAT.MacroTipologia

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            D.Mese,
            D.Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        CROSS JOIN Months D
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio AND DD.Mese = D.Mese
        GROUP BY CAT.CapoArea,
            D.Mese,
            D.Mese_IT

        UNION ALL

        SELECT
            CAT.CapoArea,
            N'Totale complessivo' AS Tipologia,
            13 AS Mese,
            N'Totale' AS Mese_IT,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.QuoteTotali ELSE NULL END) AS QuoteTotaliEsercizioPrecedente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioCorrente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioCorrente,
            SUM(CASE WHEN CE.CodiceEsercizio = @CodiceEsercizioPrecedente THEN DD.ImportoTotale ELSE NULL END) AS ImportoTotaleEsercizioPrecedente

        FROM CapiAreaTipologie CAT
        CROSS JOIN CodiciEsercizio CE
        LEFT JOIN DataDetail DD ON DD.CapoArea = CAT.CapoArea AND DD.Tipologia = CAT.Tipologia AND DD.CodiceEsercizio = CE.CodiceEsercizio
        GROUP BY CAT.CapoArea
    )
    SELECT
        T.CapoArea,
        T.Tipologia,
        T.Mese,
        T.Mese_IT,
        T.QuoteTotaliEsercizioCorrente,
        T.QuoteTotaliEsercizioPrecedente,
        T.ImportoTotaleEsercizioCorrente,
        T.ImportoTotaleEsercizioPrecedente,
        T.DeltaVsEsercizioPrecedente,
        T.rnCapoArea

    FROM (
        SELECT
            DR.CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            DR.QuoteTotaliEsercizioCorrente,
            DR.QuoteTotaliEsercizioPrecedente,
            DR.ImportoTotaleEsercizioCorrente,
            DR.ImportoTotaleEsercizioPrecedente,

            CASE WHEN COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0) = 0.0 THEN NULL ELSE DR.ImportoTotaleEsercizioCorrente / DR.ImportoTotaleEsercizioPrecedente - 1.0 END AS DeltaVsEsercizioPrecedente,
            DENSE_RANK() OVER (ORDER BY DR.CapoArea) AS rnCapoArea

        FROM DataRecap DR

        UNION ALL

        SELECT
            N'Totale' AS CapoArea,
            DR.Tipologia,
            DR.Mese,
            DR.Mese_IT,
            SUM(DR.QuoteTotaliEsercizioCorrente) AS QuoteTotaliEsercizioCorrente,
            SUM(DR.QuoteTotaliEsercizioPrecedente) AS QuoteTotaliEsercizioPrecedente,
            SUM(DR.ImportoTotaleEsercizioCorrente) AS ImportoTotaleEsercizioCorrente,
            SUM(DR.ImportoTotaleEsercizioPrecedente) AS ImportoTotaleEsercizioPrecedente,
            CASE WHEN SUM(COALESCE(DR.ImportoTotaleEsercizioPrecedente, 0.0)) = 0.0 THEN NULL ELSE SUM(DR.ImportoTotaleEsercizioCorrente) / SUM(DR.ImportoTotaleEsercizioPrecedente) - 1.0 END AS DeltaVsEsercizioPrecedente,
            999 AS rnCapoArea

        FROM DataRecap DR
        GROUP BY DR.Tipologia,
            DR.Mese,
            DR.Mese_IT

    ) T
    ORDER BY T.rnCapoArea,
        T.Tipologia,
        T.Mese;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportOrdinatoGeneraleBudget TO cesidw_reader;
GO

DECLARE @AnnoCorrente INT = 2022;
DECLARE @CapoArea NVARCHAR(60) = N'ANTONIO VAMPIRELLI';

EXEC Fact.usp_ReportOrdinatoGeneraleBudget @AnnoCorrente = @AnnoCorrente,
                                     @CapoArea = @CapoArea;
GO

/**
 * @storedprocedure Fact.usp_ReportNuoviIscrittiMasterMySolution
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportNuoviIscrittiMasterMySolution (
    @AnnoCorrente INT = NULL,
    @CapoAreaDefault NVARCHAR(60) = NULL,
    @AgenteDefault NVARCHAR(40) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @CodiceEsercizioMasterCorrente NVARCHAR(10),
            @CodiceEsercizioMasterPrecedente NVARCHAR(10);

    SELECT @AnnoCorrente = YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP));

    SELECT @CodiceEsercizioMasterCorrente = CONVERT(NVARCHAR(4), @AnnoCorrente) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente + 1);

    WITH IscrizioniMaster
    AS (
        SELECT
            D.PKCliente,
            A.CategoriaMaster,
            A.CodiceEsercizioMaster AS CodiceEsercizio,
            MAX(D.PKDataDocumento) AS DataUltimaFattura,
            COUNT(1) AS NumeroIscritti,
            SUM(D.ImportoTotale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
            AND (
                @CapoAreaDefault IS NULL
                OR C.CapoAreaDefault = @CapoAreaDefault
            )
            AND (
                @AgenteDefault IS NULL
                OR C.AgenteDefault = @AgenteDefault
            )
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
            AND A.CategoriaMaster = N'Master MySolution'
            AND A.CodiceEsercizioMaster = @CodiceEsercizioMasterCorrente
        WHERE D.IDProfilo = N'ORDSEM'
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY D.PKCliente,
            A.CategoriaMaster,
            A.CodiceEsercizioMaster
    ),
    ClientiMaster
    AS (
        SELECT DISTINCT
            IM.PKCliente
        FROM IscrizioniMaster IM
    )
    SELECT
        C.PKCliente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.IsAbbonato,
        C.CapoAreaDefault,
        C.AgenteDefault,
        C.Email,
        C.Telefono,
        C.Cellulare,
        C.Provincia,
        IM.DataUltimaFattura,
        COALESCE(CONVERT(NVARCHAR(10), IM.DataUltimaFattura, 103), N'') AS DataUltimaFatturaDescrizione

    FROM ClientiMaster CM
    INNER JOIN Dim.Cliente C ON C.PKCliente = CM.PKCliente
    INNER JOIN IscrizioniMaster IM ON IM.PKCliente = CM.PKCliente

    ORDER BY C.CodiceCliente;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportNuoviIscrittiMasterMySolution TO cesidw_reader;
GO

DECLARE @AnnoCorrente INT = 2022;
DECLARE @CapoAreaDefault NVARCHAR(60) = NULL;
DECLARE @AgenteDefault NVARCHAR(40) = NULL;

EXEC Fact.usp_ReportNuoviIscrittiMasterMySolution @AnnoCorrente = @AnnoCorrente,
                                     @CapoAreaDefault = @CapoAreaDefault,
                                     @AgenteDefault = @AgenteDefault;
GO

/**
 * @storedprocedure Fact.usp_ReportCrediti
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportCrediti (
    @Anno INT = NULL,
    @CodiceFiscale NVARCHAR(50) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@Anno IS NULL) SELECT @Anno = YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP));

    SELECT
        UPPER(CR.CodiceFiscale) AS CodiceFiscale,
        UPPER(CR.Cognome) AS Cognome,
        UPPER(CR.Nome) AS Nome,
        C.TipoCorso,
        C.Corso,
        C.Giornata,
        C.PKDataInizioCorso,
        DIC.Data_IT AS DataInizioCorso,
        C.OraInizioCorso,
        CR.Crediti,
        CR.TipoCrediti,
        CR.StatoCrediti,
        CR.EnteAccreditante,
        CR.Professione,
        REPLACE(REPLACE('https://webinar.mysolution.it/util/attestatoPDF?C=%CF%&S=%C%', '%CF%', CR.CodiceFiscale), '%C%', C.IDCorso) AS URLAttestato,
        CR.CodiceMateria

    FROM Fact.Crediti CR
    INNER JOIN Dim.Data DC ON DC.PKData = CR.PKDataCreazione
        AND DC.Anno = @Anno
    INNER JOIN Dim.Corso C ON C.PKCorso = CR.PKCorso
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Data DIC ON DIC.PKData = C.PKDataInizioCorso
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND (
            @CodiceFiscale IS NULL
            OR CR.CodiceFiscale = @CodiceFiscale
        )
    ORDER BY CR.CodiceFiscale,
        C.PKDataInizioCorso DESC,
        C.OraInizioCorso DESC;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportCrediti TO cesidw_reader;
GO

DECLARE @Anno INT = NULL;
DECLARE @CodiceFiscale NVARCHAR(50) = NULL;

EXEC Fact.usp_ReportCrediti @Anno = @Anno,
    @CodiceFiscale = @CodiceFiscale;
GO

/*
SELECT DISTINCT
    CR.AnnoCreazione AS Anno,
    CR.AnnoCreazione AS AnnoDescrizione

FROM Fact.Crediti CR
WHERE CR.IsDeleted = CAST(0 AS BIT)
ORDER BY CR.AnnoCreazione DESC;

DECLARE @Anno INT = 2024;

WITH CreditiAnnoCorrente
AS (
    SELECT
        UPPER(CR.CodiceFiscale) AS CodiceFiscale,
        UPPER(CR.Cognome + N' ' + CR.Nome + N' (' + CR.CodiceFiscale + N')') AS CodiceFiscaleDescrizione,
        SUM(CR.Crediti) AS Crediti

    FROM Fact.Crediti CR
    WHERE CR.IsDeleted = CAST(0 AS BIT)
        AND CR.AnnoCreazione = @Anno
    GROUP BY UPPER (CR.CodiceFiscale),
        UPPER (CR.Cognome + N' ' + CR.Nome + N' (' + CR.CodiceFiscale + N')')
),
CodiceFiscaleDettaglio
AS (
    SELECT
        CAC.CodiceFiscale,
        CAC.CodiceFiscaleDescrizione,
        CAC.Crediti,
        ROW_NUMBER() OVER (PARTITION BY CAC.CodiceFiscale ORDER BY CAC.Crediti DESC, CAC.CodiceFiscaleDescrizione) AS rn

    FROM CreditiAnnoCorrente CAC
)
SELECT
    CFD.CodiceFiscale,
    CFD.CodiceFiscaleDescrizione

FROM CodiceFiscaleDettaglio CFD
WHERE CFD.rn = 1
ORDER BY CodiceFiscaleDescrizione;
GO
*/
