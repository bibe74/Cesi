
/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazione
*/

CREATE   PROCEDURE Fact.usp_ReportFatturatoFormazione (
    @CodiceEsercizio CHAR(4),
    @CapoArea NVARCHAR(60),
    @Agente NVARCHAR(60)
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@CodiceEsercizio IS NULL)
    BEGIN

        SELECT @CodiceEsercizio = CAST(YEAR(CURRENT_TIMESTAMP) AS CHAR(4));

    END;

    SELECT
        C.Regione,
        C.Provincia,
        DR.Mese,
        DR.Mese_IT AS MeseDescrizione,
        SUM(D.ImportoTotale) AS ImportoTotale

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
        AND (
            @CapoArea IS NULL
            OR C.CapoAreaDefault = @CapoArea
        )
        AND (
            @Agente IS NULL
            OR C.AgenteDefault = @Agente
        )
    WHERE D.CodiceEsercizio = @CodiceEsercizio
        AND D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.Regione,
        C.Provincia,
        DR.Mese,
        DR.Mese_IT
    ORDER BY C.Regione,
        C.Provincia,
        DR.Mese;

END;

GO

