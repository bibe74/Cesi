
CREATE VIEW Landing.COMETA_MySolutionUsersView
AS
WITH TableData
AS (
    SELECT

        LOWER(EMail) AS Email,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            LOWER(EMail),
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            codice,
            RagioneSociale,
            indirizzo,
            cap,
            localita,
            provincia,
            nazione,
            cod_fiscale,
            par_iva,
            num_progressivo,
            num_documento,
            data_documento,
            data_inizio_contratto,
            data_fine_contratto,
            HaSconto,
            Nome,
            Cognome,
            Quote,
            telefono_descrizione,
            id_telefono,
            id_sog_commerciale,
            tipo,
            id_documento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        HaSconto,
        Nome,
        Cognome,
        Quote,
        telefono_descrizione,
        id_telefono,
        id_sog_commerciale,
        tipo,
        id_documento,

        ROW_NUMBER() OVER (PARTITION BY EMail ORDER BY data_inizio_contratto DESC, id_anagrafica DESC) AS rn

    FROM COMETA.MySolutionUsers
    WHERE LOWER(EMail) <> N'[DA INSERIRE]'
)
SELECT
    -- Chiavi
    TD.EMail,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_anagrafica,
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.RagioneSociale COLLATE DATABASE_DEFAULT AS RagioneSociale,
    TD.indirizzo COLLATE DATABASE_DEFAULT AS indirizzo,
    TD.cap COLLATE DATABASE_DEFAULT AS cap,
    TD.localita COLLATE DATABASE_DEFAULT AS localita,
    TD.provincia COLLATE DATABASE_DEFAULT AS provincia,
    TD.nazione COLLATE DATABASE_DEFAULT AS nazione,
    TD.cod_fiscale COLLATE DATABASE_DEFAULT AS cod_fiscale,
    TD.par_iva COLLATE DATABASE_DEFAULT AS par_iva,
    TD.num_progressivo,
    TD.num_documento COLLATE DATABASE_DEFAULT AS num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.HaSconto,
    TD.Nome COLLATE DATABASE_DEFAULT AS Nome,
    TD.Cognome COLLATE DATABASE_DEFAULT AS Cognome,
    TD.Quote,
    TD.telefono_descrizione COLLATE DATABASE_DEFAULT AS telefono_descrizione,
    TD.id_telefono,
    TD.id_sog_commerciale,
    TD.tipo COLLATE DATABASE_DEFAULT AS tipo,
    TD.id_documento

FROM TableData TD
WHERE TD.rn = 1;

GO

