CREATE   PROCEDURE Fact.usp_Reload_AccesiUltimi3Mesi
AS
BEGIN

    SET NOCOUNT ON;

    SET DATEFIRST 1;

    DECLARE @PKDataFinePeriodo DATE;

    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(CURRENT_TIMESTAMP AS DATE));

    SET @PKDataFinePeriodo = DATEADD(DAY, 7-DATEPART(WEEKDAY, @Yesterday), @Yesterday);

    DROP TABLE IF EXISTS Fact.AccessiUltimi3Mesi;

    SELECT
        A.PKCliente,
        SUM(A.NumeroAccessi) AS NumeroAccessi,
        SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
        COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

    INTO Fact.AccessiUltimi3Mesi

    FROM Fact.Accessi A
    WHERE A.PKData BETWEEN DATEADD(MONTH, -3, @PKDataFinePeriodo) AND @PKDataFinePeriodo
    GROUP BY A.PKCliente;

END;

GO

