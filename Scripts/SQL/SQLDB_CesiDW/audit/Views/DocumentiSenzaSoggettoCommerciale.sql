
/**
 * @view audit.DocumentiSenzaSoggettoCommerciale
 * @description Dettaglio documenti con soggetto commerciale non valido
*/

CREATE   VIEW audit.DocumentiSenzaSoggettoCommerciale
AS
SELECT
    D.*
FROM Landing.COMETA_Documento D
LEFT JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
WHERE SC.id_sog_commerciale IS NULL;

GO

