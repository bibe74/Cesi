
/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneDettaglioCorsi
*/

CREATE   PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglioCorsi (
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

