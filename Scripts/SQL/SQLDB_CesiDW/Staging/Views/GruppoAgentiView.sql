
CREATE VIEW Staging.GruppoAgentiView
AS
WITH TableData
AS (
    SELECT
        GA.id_gruppo_agenti,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.id_gruppo_agenti,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.codice,
            GA.descrizione,
            GA.id_sog_com_capo_area,
            ACA.rag_soc_1,
            GA.id_sog_com_agente,
            AA.rag_soc_1,
            GA.id_sog_com_sub_agente,
            ASA.rag_soc_1,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        GA.codice AS IDGruppoAgenti,
        GA.descrizione AS GruppoAgenti,
        GA.id_sog_com_capo_area AS IDSoggettoCommercialeCapoArea,
        COALESCE(ACA.rag_soc_1, CASE WHEN GA.id_sog_com_capo_area IS NULL THEN N'' ELSE N'<???>' END) AS CapoArea,
        GA.id_sog_com_agente AS IDSoggettoCommercialeAgente,
        COALESCE(AA.rag_soc_1, CASE WHEN GA.id_sog_com_agente IS NULL THEN N'' ELSE N'<???>' END) AS Agente,
        GA.id_sog_com_sub_agente AS IDSoggettoCommercialeSubagente,
        COALESCE(ASA.rag_soc_1, CASE WHEN GA.id_sog_com_sub_agente IS NULL THEN N'' ELSE N'<???>' END) AS Subagente

    FROM Landing.COMETA_Gruppo_Agenti GA
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCCA ON SCCA.id_sog_commerciale = GA.id_sog_com_capo_area
    LEFT JOIN Landing.COMETA_Anagrafica ACA ON ACA.id_anagrafica = SCCA.id_anagrafica
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCA ON SCA.id_sog_commerciale = GA.id_sog_com_agente
    LEFT JOIN Landing.COMETA_Anagrafica AA ON AA.id_anagrafica = SCA.id_anagrafica
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCSA ON SCSA.id_sog_commerciale = GA.id_sog_com_sub_agente
    LEFT JOIN Landing.COMETA_Anagrafica ASA ON ASA.id_anagrafica = SCSA.id_anagrafica
)
SELECT
    -- Chiavi
    TD.id_gruppo_agenti,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.IDGruppoAgenti,
    TD.GruppoAgenti,
    --TD.IDSoggettoCommercialeCapoArea,
    TD.CapoArea,
    --TD.IDSoggettoCommercialeAgente,
    TD.Agente,
    --TD.IDSoggettoCommercialeSubagente,
    TD.Subagente

FROM TableData TD;

GO

