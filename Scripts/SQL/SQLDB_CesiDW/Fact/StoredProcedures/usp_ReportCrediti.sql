
/**
 * @storedprocedure Fact.usp_ReportCrediti
*/

CREATE   PROCEDURE Fact.usp_ReportCrediti (
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

