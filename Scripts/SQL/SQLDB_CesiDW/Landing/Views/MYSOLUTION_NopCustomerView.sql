CREATE VIEW Landing.MYSOLUTION_NopCustomerView
AS
WITH TableData
AS (
    SELECT
        C.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Username,
            C.Email,
            C.Active,
            C.Deleted,
            C.BillingAddress_Id,
            C.ShippingAddress_Id,
            C.IdCometa,
            C.DateExpiration,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        C.Username,
        C.Email,
        C.Active,
        C.Deleted,
        C.BillingAddress_Id,
        C.ShippingAddress_Id,
        C.IdCometa,
        C.DateExpiration

    FROM MySOLUTION.Customer C
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
    TD.Username,
    TD.Email,
    TD.Active,
    TD.Deleted,
    TD.BillingAddress_Id,
    TD.ShippingAddress_Id,
    TD.IdCometa,
    TD.DateExpiration

FROM TableData TD;

GO

