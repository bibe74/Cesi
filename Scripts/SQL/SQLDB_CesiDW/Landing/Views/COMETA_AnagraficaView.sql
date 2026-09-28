
CREATE VIEW Landing.COMETA_AnagraficaView
AS
WITH TableData
AS (
    SELECT
        id_anagrafica,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            rag_soc_1,
            rag_soc_2,
            indirizzo,
            cap,
            localita,
            provincia,
            nazione,
            cod_fiscale,
            par_iva,
            indirizzo2,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        rag_soc_1,
        rag_soc_2,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        indirizzo2

    FROM COMETA.Anagrafica
)
SELECT
    -- Chiavi
    TD.id_anagrafica,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.rag_soc_1 COLLATE DATABASE_DEFAULT AS rag_soc_1,
    TD.rag_soc_2 COLLATE DATABASE_DEFAULT AS rag_soc_2,
    TD.indirizzo COLLATE DATABASE_DEFAULT AS indirizzo,
    TD.cap COLLATE DATABASE_DEFAULT AS cap,
    TD.localita COLLATE DATABASE_DEFAULT AS localita,
    TD.provincia COLLATE DATABASE_DEFAULT AS provincia,
    TD.nazione COLLATE DATABASE_DEFAULT AS nazione,
    TD.cod_fiscale COLLATE DATABASE_DEFAULT AS cod_fiscale,
    TD.par_iva COLLATE DATABASE_DEFAULT AS par_iva,
    TD.indirizzo2 COLLATE DATABASE_DEFAULT AS indirizzo2

FROM TableData TD;

GO

