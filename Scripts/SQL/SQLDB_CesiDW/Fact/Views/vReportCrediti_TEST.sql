
/**
 * @view Fact.vReportCrediti_TEST
 * @description
*/

CREATE   VIEW Fact.vReportCrediti_TEST
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

