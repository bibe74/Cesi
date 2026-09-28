
CREATE PROCEDURE MYSOLUTION.usp_Merge_NopCustomer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_NopCustomer AS TGT
    USING Landing.MYSOLUTION_NopCustomerView (NOLOCK) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Username = SRC.Username,
        TGT.Email = SRC.Email,
        TGT.Active = SRC.Active,
        TGT.Deleted = SRC.Deleted,
        TGT.BillingAddress_Id = SRC.BillingAddress_Id,
        TGT.ShippingAddress_Id = SRC.ShippingAddress_Id,
        TGT.IdCometa = SRC.IdCometa,
        TGT.DateExpiration = SRC.DateExpiration

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
    
        Username,
        Email,
        Active,
        Deleted,
        BillingAddress_Id,
        ShippingAddress_Id,
        IdCometa,
        DateExpiration
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
        'Landing.MYSOLUTION_NopCustomer' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

