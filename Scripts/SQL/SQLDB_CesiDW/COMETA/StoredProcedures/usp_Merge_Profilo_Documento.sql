
CREATE PROCEDURE COMETA.usp_Merge_Profilo_Documento
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Profilo_Documento AS TGT
    USING Landing.COMETA_Profilo_DocumentoView (nolock) AS SRC
    ON SRC.id_prof_documento = TGT.id_prof_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.tipo_registro = SRC.tipo_registro

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_prof_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        tipo_registro
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
        'Landing.COMETA_Profilo_Documento' AS full_olap_table_name,
        'id_prof_documento = ' + CAST(COALESCE(inserted.id_prof_documento, deleted.id_prof_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

