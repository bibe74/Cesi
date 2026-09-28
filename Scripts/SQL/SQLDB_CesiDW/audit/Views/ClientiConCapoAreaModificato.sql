CREATE   VIEW audit.ClientiConCapoAreaModificato
AS
SELECT TOP (100)
    C.CodiceCliente,
    C.RagioneSociale,
    C.Email,
    C.IDProvincia,
    C.Localita,
    C.CAP,
    C.CapoAreaDefault,
    GA.CapoArea

FROM Dim.Cliente C
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
    AND GA.CapoArea <> N''
WHERE C.CapoAreaDefault <> GA.CapoArea
ORDER BY C.CodiceCliente,
    C.RagioneSociale;

GO

