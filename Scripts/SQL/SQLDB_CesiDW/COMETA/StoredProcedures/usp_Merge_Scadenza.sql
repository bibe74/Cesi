
CREATE PROCEDURE COMETA.usp_Merge_Scadenza
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Scadenza AS TGT
    USING Landing.COMETA_ScadenzaView (nolock) AS SRC
    ON SRC.id_scadenza = TGT.id_scadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.tipo_scadenza = SRC.tipo_scadenza,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.data_scadenza = SRC.data_scadenza,
        TGT.importo = SRC.importo,
        TGT.stato_scadenza = SRC.stato_scadenza,
        TGT.esito_pagamento = SRC.esito_pagamento,
        TGT.id_documento = SRC.id_documento

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_scadenza,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        tipo_scadenza,
        id_sog_commerciale,
        data_scadenza,
        importo,
        stato_scadenza,
        esito_pagamento,
        id_documento
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
        'Landing.COMETA_Scadenza' AS full_olap_table_name,
        'id_scadenza = ' + CAST(COALESCE(inserted.id_scadenza, deleted.id_scadenza) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

