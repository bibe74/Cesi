USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @view Landing.vCustomerPartecipants
 * @description

*/

CREATE OR ALTER VIEW Landing.vCustomerPartecipants
AS
SELECT
    P.Email AS PartecipantEmail,
    C.Id,
    C.Username,
    C.Email,
    COALESCE(C.Username, C.Email) AS CustomerEmail,
    C.IdCometa,
    C.AdminComment,
    C.IsTaxExempt,
    C.HasShoppingCartItems,
    C.Active,
    C.Deleted,
    C.IsSystemAccount,
    C.SystemName,
    C.LastIpAddress,
    C.CreatedOnUtc,
    C.LastLoginDateUtc,
    C.LastActivityDateUtc,
    C.CustomerGuid,
    C.EmailToRevalidate,
    C.AffiliateId,
    C.VendorId,
    C.RequireReLogin,
    C.FailedLoginAttempts,
    C.CannotLoginUntilDateUtc,
    C.RegisteredInStoreId,
    C.BillingAddress_Id,
    C.ShippingAddress_Id,
    C.MysolutionSubscriptionQuote,
    C.SendRiqualification,
    C.IsSpecial,
    C.DateExpiration

FROM Landing.MYSOLUTION_Customer C
INNER JOIN Landing.MYSOLUTION_CustomerPartecipants CP on C.id = CP.Customer_Id
    AND CP.IsDeleted = CAST(0 AS BIT)
INNER JOIN Landing.MYSOLUTION_Partecipant P ON P.id = CP.Partecipant_Id
    AND P.IsDeleted = CAST(0 AS BIT)
WHERE C.IsDeleted = CAST(0 AS BIT);
GO
