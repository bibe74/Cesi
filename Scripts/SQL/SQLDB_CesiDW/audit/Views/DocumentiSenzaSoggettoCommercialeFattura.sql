
/**
 * @view audit.DocumentiSenzaSoggettoCommercialeFattura
 * @description Dettaglio documenti con soggetto commerciale fattura non valido
*/

CREATE   VIEW audit.DocumentiSenzaSoggettoCommercialeFattura
AS
SELECT
    D.*
FROM Landing.COMETA_Documento D
LEFT JOIN Landing.COMETA_SoggettoCommerciale SCF ON SCF.id_sog_commerciale = D.id_sog_commerciale_fattura
WHERE D.id_sog_commerciale_fattura IS NOT NULL
    AND SCF.id_sog_commerciale IS NULL;

GO

