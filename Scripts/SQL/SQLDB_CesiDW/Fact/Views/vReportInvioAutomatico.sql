
/**
 * @view Fact.vReportInvioAutomatico
*/

CREATE   VIEW Fact.vReportInvioAutomatico
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
        N'gabriella.mottica@cesimultimedia.it;cipriani@cesimultimedia.it;paola.turolla@cesimultimedia.it;andrea.giuggioli@cesimultimedia.it;giada.lidonnici@cesimultimedia.it;serena.leso@cesimultimedia.it;nicola.mastropietro@cesimultimedia.it;paolo.dosi@cesimultimedia.it;valentina.cipriani@cesimultimedia.it',
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
        AND ICA.InvioEmail = CAST(1 AS BIT)
    WHERE C.HasRoleMySolutionDemo = CAST(1 AS BIT)
        AND ICA.Email IS NOT NULL

    UNION ALL

    SELECT
        N'Accessi Demo',
        N'gabriella.mottica@cesimultimedia.it;cipriani@cesimultimedia.it;paola.turolla@cesimultimedia.it;andrea.giuggioli@cesimultimedia.it;giada.lidonnici@cesimultimedia.it;serena.leso@cesimultimedia.it;nicola.mastropietro@cesimultimedia.it;paolo.dosi@cesimultimedia.it;valentina.cipriani@cesimultimedia.it',
        N'Report Accessi Demo',
        NULL
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

