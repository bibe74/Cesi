
CREATE   PROCEDURE WEBINARS.usp_Merge_CreditoTipologia
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_CreditoTipologia AS TGT
    USING Landing.WEBINARS_CreditoTipologiaView (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.Ordine = SRC.Ordine,
        TGT.Tipo = SRC.Tipo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Ordine,
        Tipo
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
        'Landing.WEBINARS_CreditoTipologia' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

