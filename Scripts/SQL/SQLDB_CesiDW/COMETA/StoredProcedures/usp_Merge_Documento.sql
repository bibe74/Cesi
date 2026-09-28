CREATE PROCEDURE COMETA.usp_Merge_Documento
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Documento AS TGT
    USING Landing.COMETA_DocumentoView (NOLOCK) AS SRC
    ON SRC.id_documento = TGT.id_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.id_prof_documento = SRC.id_prof_documento,
        TGT.id_registro = SRC.id_registro,
        TGT.data_registrazione = SRC.data_registrazione,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_competenza = SRC.data_competenza,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.id_sog_commerciale_fattura = SRC.id_sog_commerciale_fattura,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.libero_4 = SRC.libero_4,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.id_libero_1 = SRC.id_libero_1,
        TGT.id_libero_2 = SRC.id_libero_2,
        TGT.id_libero_3 = SRC.id_libero_3,
        TGT.id_tipo_fatturazione = SRC.id_tipo_fatturazione,
        TGT.id_con_pagamento = SRC.id_con_pagamento,
        TGT.data_disdetta = SRC.data_disdetta,
        TGT.motivo_disdetta = SRC.motivo_disdetta,
        TGT.rinnovo_automatico = SRC.rinnovo_automatico,
        TGT.note_intestazione = SRC.note_intestazione,
        TGT.note_decisionali = SRC.note_decisionali


    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_Documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        
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
        'Landing.COMETA_Documento' AS full_olap_table_name,
        'id_documento = ' + CAST(COALESCE(inserted.id_documento, deleted.id_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

