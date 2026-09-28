
/**
 * @storedprocedure Fact.usp_ReportOrdinatoGenerale
*/

CREATE   PROCEDURE Fact.usp_ReportOrdinatoGenerale (
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

