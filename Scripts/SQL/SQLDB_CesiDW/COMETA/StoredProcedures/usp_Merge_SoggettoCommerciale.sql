
CREATE PROCEDURE COMETA.usp_Merge_SoggettoCommerciale
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_SoggettoCommerciale AS TGT
    USING Landing.COMETA_SoggettoCommercialeView (nolock) AS SRC
    ON SRC.id_sog_commerciale = TGT.id_sog_commerciale

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.codice = SRC.codice,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.tipo = SRC.tipo,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.id_cat_com_sc = SRC.id_cat_com_sc,
        TGT.rnIDSoggettoCommercialeDESC = SRC.rnIDSoggettoCommercialeDESC

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_sog_commerciale,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        id_anagrafica,
        tipo,
        id_gruppo_agenti,
        id_cat_com_sc,
        rnIDSoggettoCommercialeDESC
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(32), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_SoggettoCommerciale' AS full_olap_table_name,
        'id_sog_commerciale = ' + CAST(COALESCE(inserted.id_sog_commerciale, deleted.id_sog_commerciale) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

