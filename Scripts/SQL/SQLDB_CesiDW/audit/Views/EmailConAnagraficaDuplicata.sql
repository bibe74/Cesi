
/**
 * @view audit.EmailConAnagraficaDuplicata
 * @description Dettaglio email assegnate a più di una anagrafica
*/

CREATE   VIEW audit.EmailConAnagraficaDuplicata
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

