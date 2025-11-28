USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

IF NOT EXISTS(SELECT name FROM sys.schemas WHERE name = 'vZOHO')
BEGIN

    CREATE SCHEMA vZOHO AUTHORIZATION dbo;

END;
GO

/**
 * @view vZOHO.Clienti
*/

CREATE OR ALTER VIEW vZOHO.Clienti
AS
SELECT
    C.IDSoggettoCommerciale,
    C.Email,
    C.IDAnagraficaCometa,
    C.ProvenienzaAnagrafica,
    C.CodiceCliente,
    C.RagioneSociale,
    CASE WHEN GA.CapoArea = N'' THEN 0 ELSE 1 END AS HasCapoArea,
    CASE WHEN GA.CapoArea = N'' THEN C.CapoAreaDefault ELSE GA.CapoArea END AS AgenteZoho,
    C.CodiceFiscale,
    C.PartitaIVA,
    C.Indirizzo,
    C.CAP,
    C.Localita,
    C.Provincia,
    C.IsAttivo,
    C.IsAbbonato,
    C.Professione

FROM Dim.Cliente C
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
    AND GA.IsDeleted = CAST(0 AS BIT)
WHERE C.PKCliente > 0
    AND C.IsDeleted = CAST(0 AS BIT);
GO

SELECT * FROM vZOHO.Clienti;
GO
