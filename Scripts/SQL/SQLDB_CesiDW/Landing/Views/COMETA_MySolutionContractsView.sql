
CREATE VIEW Landing.COMETA_MySolutionContractsView
AS
WITH MySolutionContractsDetail
AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_riga_documento ORDER BY EMail) AS rn
    FROM COMETA.MySolutionContracts
),
TableData
AS (
    SELECT
        id_riga_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_riga_documento,
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
            EMail,
            num_progressivo,
            num_documento,
            data_documento,
            data_inizio_contratto,
            data_fine_contratto,
            Nome,
            Cognome,
            Quote,
            id_sog_commerciale,
            tipo,
            id_documento,
            descrizione,
            id_articolo,
            prezzo,
            sconto,
            prezzo_netto,
            prezzo_netto_ivato,
            note_intestazione,
            data_disdetta,
            motivo_disdetta,
            pec,
            CodiceArticolo,
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
        EMail,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        Nome,
        Cognome,
        Quote,
        id_sog_commerciale,
        tipo,
        id_documento,
        descrizione,
        id_articolo,
        prezzo,
        sconto,
        prezzo_netto,
        prezzo_netto_ivato,
        note_intestazione,
        data_disdetta,
        motivo_disdetta,
        pec,
        CodiceArticolo

    FROM MySolutionContractsDetail
    WHERE rn = 1
)
SELECT
    -- Chiavi
    TD.id_riga_documento,

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
    TD.codice,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva,
    TD.EMail,
    TD.num_progressivo,
    TD.num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.Nome,
    TD.Cognome,
    TD.Quote,
    TD.id_sog_commerciale,
    TD.tipo,
    TD.id_documento,
    TD.descrizione,
    TD.id_articolo,
    TD.prezzo,
    TD.sconto,
    TD.prezzo_netto,
    TD.prezzo_netto_ivato,
    TD.note_intestazione,
    TD.data_disdetta,
    TD.motivo_disdetta,
    TD.pec,
    TD.CodiceArticolo

FROM TableData TD;

GO

