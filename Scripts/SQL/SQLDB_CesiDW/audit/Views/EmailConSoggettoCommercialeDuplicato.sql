
/**
 * @view audit.EmailConSoggettoCommercialeDuplicato
 * @description Dettaglio email assegnate a più di un soggetto commerciale
 */

CREATE   VIEW audit.EmailConSoggettoCommercialeDuplicato
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

