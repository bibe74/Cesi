USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @view audit.VerificaEmailDuplicate
 * @description Email assegnate a più di un soggetto commerciale e/o a più di una anagrafica
*/

CREATE OR ALTER VIEW audit.VerificaEmailDuplicate
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

/**
 * @view audit.EmailConSoggettoCommercialeDuplicato
 * @description Dettaglio email assegnate a più di un soggetto commerciale
 */

CREATE OR ALTER VIEW audit.EmailConSoggettoCommercialeDuplicato
AS
SELECT TOP (100) PERCENT
    VED.Email,
    SC.id_sog_commerciale,
    SC.codice,
    SC.id_anagrafica,
    SC.tipo,
    A.rag_soc_1,
    A.rag_soc_2,
    A.indirizzo,
    A.cap,
    A.localita,
    A.provincia,
    A.nazione,
    A.cod_fiscale,
    A.par_iva,
    A.indirizzo2

FROM audit.VerificaEmailDuplicate VED
INNER JOIN Landing.COMETA_Telefono T ON T.num_riferimento = VED.Email
    AND T.tipo = 'E'
INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_anagrafica = T.id_anagrafica
INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
WHERE VED.IsSoggettoCommercialeDuplicato = 1
ORDER BY VED.Email,
    SC.id_sog_commerciale;
GO

/**
 * @view audit.EmailConAnagraficaDuplicata
 * @description Dettaglio email assegnate a più di una anagrafica
*/

CREATE OR ALTER VIEW audit.EmailConAnagraficaDuplicata
AS
SELECT TOP (100) PERCENT
    VED.Email,
    A.id_anagrafica,
    A.rag_soc_1,
    A.rag_soc_2,
    A.indirizzo,
    A.cap,
    A.localita,
    A.provincia,
    A.nazione,
    A.cod_fiscale,
    A.par_iva,
    A.indirizzo2

FROM audit.VerificaEmailDuplicate VED
INNER JOIN Landing.COMETA_Telefono T ON T.num_riferimento = VED.Email
    AND T.tipo = 'E'
INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = T.id_anagrafica
WHERE VED.IsAnagraficaDuplicata = 1
ORDER BY VED.Email,
    A.id_anagrafica;
GO

/**
 * @view audit.DocumentiSenzaSoggettoCommerciale
 * @description Dettaglio documenti con soggetto commerciale non valido
*/

CREATE OR ALTER VIEW audit.DocumentiSenzaSoggettoCommerciale
AS
SELECT
    D.*
FROM Landing.COMETA_Documento D
LEFT JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
WHERE SC.id_sog_commerciale IS NULL;
GO

/**
 * @view audit.DocumentiSenzaSoggettoCommercialeFattura
 * @description Dettaglio documenti con soggetto commerciale fattura non valido
*/

CREATE OR ALTER VIEW audit.DocumentiSenzaSoggettoCommercialeFattura
AS
SELECT
    D.*
FROM Landing.COMETA_Documento D
LEFT JOIN Landing.COMETA_SoggettoCommerciale SCF ON SCF.id_sog_commerciale = D.id_sog_commerciale_fattura
WHERE D.id_sog_commerciale_fattura IS NOT NULL
    AND SCF.id_sog_commerciale IS NULL;
GO

/**
 * @view audit.ClientiConCapoAreaModificato
 * @description Clienti con CapoArea diverso dal CapoAreaDefault
*/

CREATE OR ALTER VIEW audit.ClientiConCapoAreaModificato
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
