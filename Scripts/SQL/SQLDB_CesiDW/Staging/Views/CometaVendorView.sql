
/**
 * @table Staging.CometaVendor
 * @description 

 * @depends Landing.COMETA_SoggettoCommerciale
 * @depends Landing.COMETA_Anagrafica

SELECT TOP (1) * FROM Landing.COMETA_SoggettoCommerciale;
SELECT TOP (1) * FROM Landing.COMETA_Anagrafica;
*/

CREATE   VIEW Staging.CometaVendorView
AS
WITH TableData
AS (
    SELECT
        SC.id_sog_commerciale,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
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
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SC.codice,
        SC.id_anagrafica,
        SC.tipo,
        COALESCE(A.rag_soc_1, N'') + COALESCE(A.rag_soc_2, N'') AS RagioneSociale,
        A.indirizzo,
        A.cap,
        A.localita,
        A.provincia,
        A.nazione,
        A.cod_fiscale,
        A.par_iva

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    WHERE SC.tipo = 'F'
        AND SC.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.id_sog_commerciale,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.codice,
    TD.id_anagrafica,
    TD.tipo,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva

FROM TableData TD;

GO

