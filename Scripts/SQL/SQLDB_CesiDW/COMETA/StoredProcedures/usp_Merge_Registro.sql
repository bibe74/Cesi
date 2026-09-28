
CREATE PROCEDURE COMETA.usp_Merge_Registro
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Registro AS TGT
    USING Landing.COMETA_RegistroView (nolock) AS SRC
    ON SRC.id_registro = TGT.id_registro

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.id_esercizio = SRC.id_esercizio,
        TGT.tipo_registro = SRC.tipo_registro,
        TGT.id_mod_registro = SRC.id_mod_registro,
        TGT.numero = SRC.numero,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_registro,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_esercizio,
        tipo_registro,
        id_mod_registro,
        numero,
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
        'Landing.COMETA_Registro' AS full_olap_table_name,
        'id_registro = ' + CAST(COALESCE(inserted.id_registro, deleted.id_registro) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

