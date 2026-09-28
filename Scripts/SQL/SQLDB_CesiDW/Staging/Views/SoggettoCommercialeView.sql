CREATE VIEW Staging.SoggettoCommercialeView
AS
WITH TableData
AS (
    SELECT
        SC.id_sog_commerciale AS IDSoggettoCommerciale,

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
            A.indirizzo2,
            GA.PKGruppoAgenti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SC.codice AS CodiceSoggettoCommerciale,
        SC.id_anagrafica AS IDAnagrafica,
        SC.tipo AS TipoSoggettoCommerciale,
        --A.rag_soc_1,
        --A.rag_soc_2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.rag_soc_1, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.rag_soc_2, N'')))))) AS RagioneSociale,
        --A.indirizzo,
        --A.indirizzo2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.indirizzo, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.indirizzo2, N'')))))) AS Indirizzo,
        COALESCE(A.cap, N'') AS CAP,
        COALESCE(A.localita, N'') AS Localita,
        COALESCE(A.provincia, N'') AS Provincia,
        COALESCE(A.nazione, N'') AS Nazione,
        COALESCE(A.cod_fiscale, N'') AS CodiceFiscale,
        COALESCE(A.par_iva, N'') AS PartitaIVA,
        --SC.id_gruppo_agenti,
        COALESCE(GA.PKGruppoAgenti, -1) AS PKGruppoAgenti

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = SC.id_gruppo_agenti
        AND SC.tipo = 'C' -- C: Cliente
    WHERE SC.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.IDSoggettoCommerciale,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.CodiceSoggettoCommerciale,
    TD.IDAnagrafica,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Nazione,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.PKGruppoAgenti

FROM TableData TD;

GO

