
CREATE PROCEDURE COMETA.usp_Merge_Documento_Riga
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Documento_Riga AS TGT
    USING Landing.COMETA_Documento_RigaView (nolock) AS SRC
    ON SRC.id_riga_documento = TGT.id_riga_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.id_documento = SRC.id_documento,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.num_riga = SRC.num_riga,
        TGT.id_articolo = SRC.id_articolo,
        TGT.descrizione = SRC.descrizione,
        TGT.totale_riga = SRC.totale_riga,
        TGT.provv_calcolata_carea = SRC.provv_calcolata_carea,
        TGT.provv_calcolata_agente = SRC.provv_calcolata_agente,
        TGT.provv_calcolata_subagente = SRC.provv_calcolata_subagente,
        TGT.id_riga_doc_provenienza = SRC.id_riga_doc_provenienza

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_riga_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_documento,
        id_gruppo_agenti,
        num_riga,
        id_articolo,
        descrizione,
        totale_riga,
        provv_calcolata_carea,
        provv_calcolata_agente,
        provv_calcolata_subagente,
        id_riga_doc_provenienza
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Documento_Riga' AS full_olap_table_name,
        'id_riga_documento = ' + CAST(COALESCE(inserted.id_riga_documento, deleted.id_riga_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

