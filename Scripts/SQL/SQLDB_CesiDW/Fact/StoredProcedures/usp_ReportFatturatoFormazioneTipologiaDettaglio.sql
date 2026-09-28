
/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio
*/

CREATE   PROCEDURE Fact.usp_ReportFatturatoFormazioneTipologiaDettaglio (
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

