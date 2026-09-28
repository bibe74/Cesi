CREATE VIEW Landing.MYSOLUTION_CustomerView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
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
            StateProvinceId,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
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

    FROM MYSOLUTION.Customer
    WHERE Active = CAST(1 AS BIT)
        AND Deleted = CAST(0 AS BIT)
        AND COALESCE(Username, Email) IS NOT NULL
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    COALESCE(TD.Username, TD.Email) AS Username,
    COALESCE(TD.Username, TD.Email) AS Email,
    TD.IdCometa,
    TD.AdminComment,
    TD.IsTaxExempt,
    TD.HasShoppingCartItems,
    TD.Active,
    TD.Deleted,
    TD.IsSystemAccount,
    TD.SystemName,
    TD.LastIpAddress,
    TD.CreatedOnUtc,
    TD.LastLoginDateUtc,
    TD.LastActivityDateUtc,
    TD.CustomerGuid,
    TD.EmailToRevalidate,
    TD.AffiliateId,
    TD.VendorId,
    TD.RequireReLogin,
    TD.FailedLoginAttempts,
    TD.CannotLoginUntilDateUtc,
    TD.RegisteredInStoreId,
    TD.BillingAddress_Id,
    TD.ShippingAddress_Id,
    TD.MysolutionSubscriptionQuote,
    TD.SendRiqualification,
    TD.IsSpecial,
    TD.DateExpiration,
    TD.StateProvinceId

FROM TableData TD;

GO

