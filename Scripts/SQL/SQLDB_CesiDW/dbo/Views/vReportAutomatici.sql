/**
 * @view dbo.vReportAutomatici
*/

CREATE   VIEW dbo.vReportAutomatici
AS
SELECT
    ReportName,
    pTo,
    '' AS pCc,
    pBcc,
    pReplyTo,
    pSubject,
    pCapoArea,
    NULL AS pAnno,
    NULL AS pCodiceFiscale

FROM Fact.vReportInvioAutomatico
WHERE ReportName IN (
    N'Accessi Demo',
    N'Accessi',
    N'Dettaglio Ordini In Scadenza',
    N'Dettaglio Ordini Mese Corrente'
)

UNION ALL SELECT
    'Fatturato Formazione - Corsi',
    'cipriani@cesimultimedia.it;angela.battaglia@cesimultimedia.it;giada.lidonnici@cesimultimedia.it;serena.leso@cesimultimedia.it;valentina.cipriani@cesimultimedia.it',
    'andrea.giuggioli@cesimultimedia.eu',
    'alberto.turelli@gmail.com',
    '',
    '@ReportName was executed at @ExecutionTime' AS pSubject,
    NULL,
    NULL,
    NULL

UNION ALL SELECT 'Crediti', pTo, '', '', 'formazione@mysolution.it', pSubject, NULL, pAnno, pCodiceFiscale FROM Fact.vReportCrediti;

GO

