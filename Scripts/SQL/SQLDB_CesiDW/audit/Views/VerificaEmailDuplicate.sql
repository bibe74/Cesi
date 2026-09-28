
/*
SELECT DISTINCT
    GA.CapoArea,
    GA.CapoArea AS CapoAreaDescrizione

FROM Fact.Accessi A
INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
INNER JOIN Bridge.ADUserCapoArea AUCA ON AUCA.CapoArea = GA.CapoArea
    AND AUCA.ADUser = @ADUser

UNION ALL SELECT NULL, N'Tutti'
FROM Import.Amministratori A
WHERE A.ADUser = @ADUser

ORDER BY CapoArea;
GO
*/

-- CesiDW_60.01_audit_views.sql

/**
 * @view audit.VerificaEmailDuplicate
 * @description Email assegnate a più di un soggetto commerciale e/o a più di una anagrafica
*/

CREATE   VIEW audit.VerificaEmailDuplicate
AS
SELECT TOP (100) PERCENT
    T.num_riferimento AS Email,
    MIN(SC.id_sog_commerciale) AS id_sog_commerciale_MIN,
    MAX(SC.id_sog_commerciale) AS id_sog_commerciale_MAX,
    COUNT(DISTINCT SC.id_sog_commerciale) AS id_sog_commerciale_COUNT,
    CASE WHEN COUNT(DISTINCT SC.id_sog_commerciale) > 1 THEN 1 ELSE 0 END AS IsSoggettoCommercialeDuplicato,
    MIN(T.id_anagrafica) AS id_anagrafica_MIN,
    MAX(T.id_anagrafica) AS id_anagrafica_MAX,
    COUNT(DISTINCT T.id_anagrafica) AS id_anagrafica_COUNT,
    CASE WHEN COUNT(DISTINCT T.id_anagrafica) > 1 THEN 1 ELSE 0 END AS IsAnagraficaDuplicata

FROM Landing.COMETA_Telefono T
INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_anagrafica = T.id_anagrafica
WHERE T.tipo = 'E'
    AND T.num_riferimento <> N''
GROUP BY T.num_riferimento
HAVING CASE WHEN COUNT(DISTINCT SC.id_sog_commerciale) > 1 THEN 1 ELSE 0 END = 1
    OR CASE WHEN COUNT(DISTINCT T.id_anagrafica) > 1 THEN 1 ELSE 0 END = 1
ORDER BY Email;

GO

