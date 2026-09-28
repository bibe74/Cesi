
CREATE   PROCEDURE WEBINARS.usp_Merge_CreditoCorso
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_CreditoCorso AS TGT
    USING Landing.WEBINARS_CreditoCorsoView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.CreditoTipologiaID = SRC.CreditoTipologiaID,
        TGT.WebinarSource = SRC.WebinarSource,
        TGT.Autocertificazione = SRC.Autocertificazione,
        TGT.Crediti = SRC.Crediti,
        TGT.Ora = SRC.Ora,
        TGT.CodiceMateria = SRC.CodiceMateria

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        CreditoTipologiaID,
        WebinarSource,
        Autocertificazione,
        Crediti,
        Ora,
        CodiceMateria
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
        'Landing.WEBINARS_CreditoCorso' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

