USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @view Fact.vOrdiniMeseInCorso
*/

CREATE OR ALTER VIEW Fact.vOrdiniMeseInCorso
AS
WITH MeseCorrente
AS (
    SELECT
        MIN(D.PKData) AS PKDataInizioMese,
        MAX(D.PKData) AS PKDataFineMese

    FROM Dim.Data CTD
    INNER JOIN Dim.Data D ON D.Anno = CTD.Anno AND D.Mese = CTD.Mese
    WHERE CTD.PKData = CAST(CURRENT_TIMESTAMP AS DATE)
),
Insoluti
AS (
    SELECT
        D.PKCliente,
        SUM(S.ImportoResiduo) AS Insoluto
    FROM Fact.Scadenze S
    INNER JOIN Fact.Documenti D ON D.PKDocumenti = S.PKDocumenti
    GROUP BY D.PKCliente
    HAVING SUM(S.ImportoResiduo) > 0.0
)
SELECT TOP (100)
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
    ----C.StatoDisdetta,
    C.MotivoDisdetta

FROM Fact.Documenti D
INNER JOIN MeseCorrente MC ON D.PKDataCompetenza BETWEEN MC.PKDataInizioMese AND MC.PKDataFineMese
INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
INNER JOIN Dim.Data DIC ON DIC.PKData = D.PKDataInizioContratto
INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
WHERE D.Profilo = N'ORDINE CLIENTE'
ORDER BY AgenteAssegnato,
    C.CodiceCliente,
    D.NumeroDocumento,
    D.NumeroRiga;
GO

/**
 * @view Fact.vReportInvioAutomatico
*/

CREATE OR ALTER VIEW Fact.vReportInvioAutomatico
AS
WITH ReportInvioAutomaticoDettaglio
AS (
    SELECT
        N'Accessi' AS ReportName,
        Email AS pTo,
        REPLACE(N'Report Accessi %AGENTE%', N'%AGENTE%', Agente) AS pSubject,
        CapoArea AS pCapoArea

    FROM Import.CapiArea
    WHERE InvioEmail = CAST(1 AS BIT)

    UNION ALL

    SELECT
        N'Accessi',
        --N'cipriani@cesimultimedia.it;paola.turolla@cesimultimedia.it;giuseppe.lobrano@cesimultimedia.com;valeria.barbaglia@cesimultimedia.it;antonio.loprevite@cesimultimedia.it;andrea.giuggioli@cesimultimedia.it;eleonora.soravia@cesimultimedia.it;valentina.borroni@cesimultimedia.it',
        N'gabriella.mottica@cesimultimedia.it;cipriani@cesimultimedia.it;paola.turolla@cesimultimedia.it;mirco.polinari@cesimultimedia.it;andrea.giuggioli@cesimultimedia.it;eleonora.soravia@cesimultimedia.it;giada.lucarini@cesimultimedia.it;angela.battaglia@cesimultimedia.it',
        N'Report Accessi',
        NULL

    UNION ALL

    SELECT DISTINCT
        N'Dettaglio Ordini Mese Corrente',
        ICA.Email,
        REPLACE(N'Dettaglio Ordini Mese Corrente %AGENTE%', N'%AGENTE%', GA.CapoArea),
        GA.CapoArea

    FROM Fact.Documenti D
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
    INNER JOIN IMPORT.CapiArea ICA ON ICA.CapoArea = GA.CapoArea
        AND ICA.InvioEmail = CAST(1 AS BIT)
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND DC.PKData BETWEEN DATEADD(DAY, 1-DATEPART(DAY, DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))), DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))) AND DATEADD(MONTH, 1, DATEADD(DAY, 1-DATEPART(DAY, DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))), DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))))
    WHERE D.Profilo = N'ORDINE CLIENTE'
        AND D.IsDeleted = CAST(0 AS BIT)

    UNION ALL

    SELECT DISTINCT
        N'Dettaglio Ordini In Scadenza',
        ICA.Email,
        REPLACE(N'Dettaglio Ordini In Scadenza %AGENTE%', N'%AGENTE%', GA.CapoArea),
        GA.CapoArea

    FROM Fact.Documenti D
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
    INNER JOIN IMPORT.CapiArea ICA ON ICA.CapoArea = GA.CapoArea
        AND ICA.InvioEmail = CAST(1 AS BIT)
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
    INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
        AND DFC.PKData BETWEEN DATEADD(DAY, 1-DATEPART(DAY, DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))), DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))) AND DATEADD(DAY, -1, DATEADD(MONTH, 3, DATEADD(DAY, 1-DATEPART(DAY, DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE))), DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE)))))
    WHERE D.Profilo = N'ORDINE CLIENTE'
        AND D.IsDeleted = CAST(0 AS BIT)

    UNION ALL

    SELECT DISTINCT
        N'Fatturato Formazione Nuovi Iscritti',
        ICA.Email,
        REPLACE(N'Fatturato Formazione Nuovi Iscritti %AGENTE%', N'%AGENTE%', C.CapoAreaDefault),
        C.CapoAreaDefault

    FROM Fact.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
    INNER JOIN IMPORT.CapiArea ICA ON ICA.CapoArea = C.CapoAreaDefault
        AND ICA.InvioEmail = CAST(1 AS BIT)
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        AND A.CategoriaMaster = N'Master MySolution'
        AND A.CodiceEsercizioMaster = CONVERT(NVARCHAR(4), YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP))) + N'/' + CONVERT(NVARCHAR(4), YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP)) + 1)
    WHERE D.IDProfilo = N'ORDSEM'
        AND D.IsDeleted = CAST(0 AS BIT)

    UNION ALL

    SELECT DISTINCT
        N'Accessi Demo',
        ICA.Email,
        REPLACE(N'Report Accessi Demo %AGENTE%', N'%AGENTE%', C.Agente),
        C.Agente

    FROM Dim.ClienteAccessi C
    LEFT JOIN Import.CapiArea ICA ON ICA.CapoArea = C.Agente
    WHERE C.HasRoleMySolutionDemo = CAST(1 AS BIT)
)
SELECT
    CONVERT(NVARCHAR(40), RIAD.ReportName) AS ReportName,
    CONVERT(NVARCHAR(500), RIAD.pTo) AS pTo,
    NULL AS pCc,
    N'alberto.turelli@gmail.com' AS pBcc,
    N'cipriani@cesimultimedia.it' AS pReplyTo,
    CONVERT(NVARCHAR(100), RIAD.pSubject) AS pSubject,
    RIAD.pCapoArea

FROM ReportInvioAutomaticoDettaglio RIAD;
GO

SELECT * FROM Fact.vReportInvioAutomatico WHERE ReportName = N'Accessi';
SELECT * FROM Fact.vReportInvioAutomatico WHERE ReportName = N'Accessi Demo';
SELECT * FROM Fact.vReportInvioAutomatico WHERE ReportName = N'Dettaglio Ordini Mese Corrente';
SELECT * FROM Fact.vReportInvioAutomatico WHERE ReportName = N'Dettaglio Ordini In Scadenza';
SELECT * FROM Fact.vReportInvioAutomatico WHERE ReportName = N'Fatturato Formazione Nuovi Iscritti';
GO

/**
 * @view Fact.vReportCrediti_TEST
 * @description
*/

CREATE OR ALTER VIEW Fact.vReportCrediti_TEST
AS
SELECT DISTINCT
    C.EMail AS pTo,
    N'Report Crediti ' + CONVERT(NVARCHAR(4), D.Anno) + ' - ' + C.Cognome + N' ' + C.Nome AS pSubject,
    D.Anno AS pAnno,
    C.CodiceFiscale AS pCodiceFiscale,
    N'Gentile Professionista, in allegato trova il report dei Crediti maturati nell''anno in corso, aggiornato ad oggi. La Segreteria potrà valutare eventuali richieste di rettifica dei Crediti inviate dal partecipante via e-mail all''indirizzo formazione@cesimultimedia.it entro 7 giorni dalla ricezione della presente.

Cordiali saluti
Team MySolution
' AS pComment

FROM Fact.Crediti C
INNER JOIN Dim.Data D ON D.PKData = C.PKDataCreazione
    AND D.Anno = YEAR(CURRENT_TIMESTAMP)
INNER JOIN Import.InvioReportCrediti IRC ON IRC.Email = C.EMail;
GO

/**
 * @view Fact.vReportCrediti
 * @description
*/

CREATE OR ALTER VIEW Fact.vReportCrediti
AS
SELECT DISTINCT
    C.EMail AS pTo,
    N'Report Crediti ' + CONVERT(NVARCHAR(4), D.Anno) + ' - ' + C.Cognome + N' ' + C.Nome AS pSubject,
    D.Anno AS pAnno,
    C.CodiceFiscale AS pCodiceFiscale,
    N'Gentile Professionista, in allegato trova il report dei Crediti maturati nell''anno in corso, aggiornato ad oggi. La Segreteria potrà valutare eventuali richieste di rettifica dei Crediti inviate dal partecipante via e-mail all''indirizzo formazione@cesimultimedia.it entro 7 giorni dalla ricezione della presente.

Cordiali saluti
Team MySolution
' AS pComment

FROM Fact.Crediti C
INNER JOIN Dim.Data D ON D.PKData = C.PKDataCreazione
    AND D.Anno = YEAR(CURRENT_TIMESTAMP)
WHERE C.Crediti > 0;
GO
