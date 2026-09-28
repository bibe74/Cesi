
CREATE VIEW Landing.COMETA_Documento_RigaView
AS
WITH TableData
AS (
    SELECT
        DR.id_riga_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_documento,
            DR.id_gruppo_agenti,
            DR.num_riga,
            DR.id_articolo,
            DR.descrizione,
            DR.totale_riga,
            DR.provv_calcolata_carea,
            DR.provv_calcolata_agente,
            DR.provv_calcolata_subagente,
            DR.id_riga_doc_provenienza,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        DR.id_documento,
        DR.id_gruppo_agenti,
        DR.num_riga,
        DR.id_articolo,
        DR.descrizione,
        DR.totale_riga,
        DR.provv_calcolata_carea,
        DR.provv_calcolata_agente,
        DR.provv_calcolata_subagente,
        DR.id_riga_doc_provenienza

    FROM COMETA.Documento_Riga_qlv DR
    INNER JOIN COMETA.Documento D ON D.id_documento = DR.id_documento
        --AND D.id_prof_documento = 1 -- 1: Ordine
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
    TD.id_documento,
    TD.id_gruppo_agenti,
    TD.num_riga,
    TD.id_articolo,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,

    -- Misure
    TD.totale_riga,
    TD.provv_calcolata_carea,
    TD.provv_calcolata_agente,
    TD.provv_calcolata_subagente,
    
    TD.id_riga_doc_provenienza

FROM TableData TD;

GO

