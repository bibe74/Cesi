
CREATE PROCEDURE COMETA.usp_Merge_Libero_3
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Libero_3 AS TGT
    USING Landing.COMETA_Libero_3View (nolock) AS SRC
    ON SRC.id_libero_3 = TGT.id_libero_3

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_libero_3,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
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
        'Landing.COMETA_Libero_3' AS full_olap_table_name,
        'id_libero_3 = ' + CAST(COALESCE(inserted.id_libero_3, deleted.id_libero_3) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

