
CREATE PROCEDURE MYSOLUTION.usp_Merge_Customer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Customer AS TGT
    USING Landing.MYSOLUTION_CustomerView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Username = SRC.Username,
        TGT.Email = SRC.Email,
        TGT.IdCometa = SRC.IdCometa,
        TGT.AdminComment = SRC.AdminComment,
        TGT.IsTaxExempt = SRC.IsTaxExempt,
        TGT.HasShoppingCartItems = SRC.HasShoppingCartItems,
        TGT.Active = SRC.Active,
        TGT.Deleted = SRC.Deleted,
        TGT.IsSystemAccount = SRC.IsSystemAccount,
        TGT.SystemName = SRC.SystemName,
        TGT.LastIpAddress = SRC.LastIpAddress,
        TGT.CreatedOnUtc = SRC.CreatedOnUtc,
        TGT.LastLoginDateUtc = SRC.LastLoginDateUtc,
        TGT.LastActivityDateUtc = SRC.LastActivityDateUtc,
        TGT.CustomerGuid = SRC.CustomerGuid,
        TGT.EmailToRevalidate = SRC.EmailToRevalidate,
        TGT.AffiliateId = SRC.AffiliateId,
        TGT.VendorId = SRC.VendorId,
        TGT.RequireReLogin = SRC.RequireReLogin,
        TGT.FailedLoginAttempts = SRC.FailedLoginAttempts,
        TGT.CannotLoginUntilDateUtc = SRC.CannotLoginUntilDateUtc,
        TGT.RegisteredInStoreId = SRC.RegisteredInStoreId,
        TGT.BillingAddress_Id = SRC.BillingAddress_Id,
        TGT.ShippingAddress_Id = SRC.ShippingAddress_Id,
        TGT.MysolutionSubscriptionQuote = SRC.MysolutionSubscriptionQuote,
        TGT.SendRiqualification = SRC.SendRiqualification,
        TGT.IsSpecial = SRC.IsSpecial,
        TGT.DateExpiration = SRC.DateExpiration,
        TGT.StateProvinceId = SRC.StateProvinceId

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
        IdCometa,
        AdminComment,
        IsTaxExempt,
        HasShoppingCartItems,
        Active,
        Deleted,
        IsSystemAccount,
        SystemName,
        LastIpAddress,
        CreatedOnUtc,
        LastLoginDateUtc,
        LastActivityDateUtc,
        CustomerGuid,
        EmailToRevalidate,
        AffiliateId,
        VendorId,
        RequireReLogin,
        FailedLoginAttempts,
        CannotLoginUntilDateUtc,
        RegisteredInStoreId,
        BillingAddress_Id,
        ShippingAddress_Id,
        MysolutionSubscriptionQuote,
        SendRiqualification,
        IsSpecial,
        DateExpiration,
        StateProvinceId
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
        'Landing.MYSOLUTION_Customer' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

