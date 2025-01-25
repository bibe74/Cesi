-- Estrarre statistica accessi, identificare un cliente Cometa con accessi che esista anche in NopCommerce

SELECT
    DC.Email,
    DC.PKCliente,
    DC.IDSoggettoCommerciale,
    DC.*,
    AM.NumeroAccessi

FROM Dim.Cliente DC
INNER JOIN Landing.MYSOLUTION_Customer MSC ON MSC.Email = DC.Email
INNER JOIN (
    SELECT
        A.PKCliente,
        SUM(A.NumeroAccessi) AS NumeroAccessi
    FROM Fact.Accessi A
    WHERE A.PKData >= CAST('20210501' AS DATE)
    GROUP BY A.PKCliente
) AM ON AM.PKCliente = DC.PKCliente
WHERE DC.ProvenienzaAnagrafica = N'COMETA'
ORDER BY AM.NumeroAccessi DESC;
GO

DECLARE @EMail NVARCHAR(120);
DECLARE @PKCliente INT;
DECLARE @IDSoggettoCommerciale INT;

SELECT TOP 1
    @EMail = DC.Email,
    @PKCliente = DC.PKCliente,
    @IDSoggettoCommerciale = DC.IDSoggettoCommerciale

FROM Dim.Cliente DC
INNER JOIN Landing.MYSOLUTION_Customer MSC ON MSC.Email = DC.Email
INNER JOIN (
    SELECT
        A.PKCliente,
        SUM(A.NumeroAccessi) AS NumeroAccessi
    FROM Fact.Accessi A
    WHERE A.PKData >= CAST('20210501' AS DATE)
    GROUP BY A.PKCliente
) AM ON AM.PKCliente = DC.PKCliente
WHERE DC.ProvenienzaAnagrafica = N'COMETA'
ORDER BY AM.NumeroAccessi DESC;

SELECT @EMail AS EMail, @PKCliente AS PKCliente, @IDSoggettoCommerciale AS IDSoggettoCommerciale;

-- Cancellare Fact.Accessi per cliente X

DELETE FROM Fact.Accessi WHERE PKCliente = @PKCliente;

-- Cancellare Fact.Documenti per cliente X

DELETE FROM Fact.Documenti WHERE PKCliente = @PKCliente;

-- Cancellare Dim.Cliente per cliente X

DELETE FROM Dim.Cliente WHERE Email = @EMail;

-- Cancellare Landing.COMETA_SoggettoCommerciale per cliente X

DELETE FROM Landing.COMETA_SoggettoCommerciale WHERE id_sog_commerciale = @IDSoggettoCommerciale;

-- Cancellare Staging.SoggettoCommerciale per cliente X

DELETE FROM Staging.SoggettoCommerciale WHERE IDSoggettoCommerciale = @IDSoggettoCommerciale;

-- EXEC Staging.usp_Reload_Cliente

EXEC Staging.usp_Reload_Cliente;

-- EXEC Dim.usp_Merge_Cliente

EXEC Dim.usp_Merge_Cliente;

-- Verifica esistenza cliente X' con provenienza NOPCOMMERCE

SELECT * FROM Dim.Cliente WHERE Email = @EMail;

-- EXEC COMETA.usp_Merge_SoggettoCommerciale

EXEC COMETA.usp_Merge_SoggettoCommerciale;

-- EXEC Staging.usp_Reload_SoggettoCommerciale

EXEC Staging.usp_Reload_SoggettoCommerciale;

-- EXEC Staging.usp_Reload_Cliente

EXEC Staging.usp_Reload_Cliente;

-- EXEC Dim.usp_Merge_Cliente

EXEC Dim.usp_Merge_Cliente;

-- Verifica esistenza cliente X e X'

SELECT * FROM Dim.Cliente WHERE Email = @EMail;
GO
