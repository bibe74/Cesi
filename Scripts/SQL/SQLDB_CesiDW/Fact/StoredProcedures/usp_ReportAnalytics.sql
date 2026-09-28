/**
 * @storedprocedure Fact.usp_ReportAnalytics
*/

CREATE   PROCEDURE Fact.usp_ReportAnalytics (
    @CapoArea NVARCHAR(60) = NULL,
    @PKDataVisitaInizio DATE = NULL,
    @PKDataVisitaFine DATE = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT @PKDataVisitaInizio = COALESCE(@PKDataVisitaInizio, DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP)));
    SELECT @PKDataVisitaFine = COALESCE(@PKDataVisitaFine, DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP)));

    SELECT
        C.Email,
        C.CodiceCliente,
        C.RagioneSociale,
        DV.Data_IT AS [Data visita],
        A.Percorso,
        A.NumeroVisite AS [Numero visite]

    FROM Fact.Analytics A
    INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
        AND GA.IsDeleted = CAST(0 AS BIT)
        AND (
            @CapoArea IS NULL
            OR GA.CapoArea = @CapoArea
        )
    INNER JOIN Dim.Data DV ON DV.PKData = A.PKDataVisita
        AND DV.PKData BETWEEN @PKDataVisitaInizio AND @PKDataVisitaFine
    WHERE A.IsDeleted = CAST(0 AS BIT)
    ORDER BY C.Email,
        A.PKDataVisita;

END;

GO

