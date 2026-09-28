CREATE VIEW Landing.COMETA_DocumentoView
AS
WITH TableData
AS (
    SELECT
        id_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            data_documento,
            num_documento,
            id_prof_documento,
            id_sog_commerciale,
            id_sog_commerciale_fattura,
            data_inizio_contratto,
            data_fine_contratto,
            id_gruppo_agenti,
            id_libero_1,
            id_libero_2,
            id_libero_3,
            libero_4,
            id_tipo_fatturazione,
            id_registro,
            data_competenza,
            data_registrazione,
            data_disdetta,
            motivo_disdetta,
            id_con_pagamento,
            rinnovo_automatico,
            note_intestazione,
            note_decisionali,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_prof_documento,
        id_registro,
        data_registrazione,
        num_documento,
        data_documento,
        data_competenza,
        id_sog_commerciale,
        id_sog_commerciale_fattura,
        id_gruppo_agenti,
        data_fine_contratto,
        libero_4,
        data_inizio_contratto,
        id_libero_1,
        id_libero_2,
        id_libero_3,
        id_tipo_fatturazione,
        data_disdetta,
        motivo_disdetta,
        id_con_pagamento,
        rinnovo_automatico,
        note_intestazione,
        note_decisionali

    FROM COMETA.Documento
    --WHERE id_prof_documento = 1 -- 1: Ordine
)
SELECT
    -- Chiavi
    TD.id_documento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_prof_documento,
    TD.id_registro,
    TD.data_registrazione,
    TD.num_documento COLLATE DATABASE_DEFAULT AS num_documento,
    TD.data_documento,
    TD.data_competenza,
    TD.id_sog_commerciale,
    TD.id_sog_commerciale_fattura,
    TD.id_gruppo_agenti,
    TD.data_fine_contratto,
    TD.libero_4 COLLATE DATABASE_DEFAULT AS libero_4,
    TD.data_inizio_contratto,
    TD.id_libero_1,
    TD.id_libero_2,
    TD.id_libero_3,
    TD.id_tipo_fatturazione,
    TD.data_disdetta,
    LEFT(TD.motivo_disdetta, 120) AS motivo_disdetta,
    TD.id_con_pagamento,
    TD.rinnovo_automatico COLLATE DATABASE_DEFAULT AS rinnovo_automatico,
    LEFT(TD.note_intestazione, 1000) COLLATE DATABASE_DEFAULT AS note_intestazione,
    LEFT(TD.note_decisionali, 1000) COLLATE DATABASE_DEFAULT AS note_decisionali

FROM TableData TD;

GO

