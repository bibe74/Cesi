/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneDettaglio
*/

CREATE   PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglio (
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
        INNER JOIN Import.ArticoloCategoriaMaster ACM ON ACM.Codice = A.Codice
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

