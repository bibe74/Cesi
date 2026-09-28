
/**
 * @storedprocedure Fact.usp_ReportNuoviIscrittiMasterMySolution
*/

CREATE   PROCEDURE Fact.usp_ReportNuoviIscrittiMasterMySolution (
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

