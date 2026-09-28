
CREATE PROCEDURE COMETA.usp_Merge_Articolo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Articolo AS TGT
    USING Landing.COMETA_ArticoloView (nolock) AS SRC
    ON SRC.id_articolo = TGT.id_articolo

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.id_cat_com_articolo = SRC.id_cat_com_articolo,
        TGT.id_cat_merceologica = SRC.id_cat_merceologica,
        TGT.des_breve = SRC.des_breve

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_Articolo,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        id_cat_com_articolo,
        id_cat_merceologica,
        des_breve
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
        'Landing.COMETA_Articolo' AS full_olap_table_name,
        'id_articolo = ' + CAST(COALESCE(inserted.id_articolo, deleted.id_articolo) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

