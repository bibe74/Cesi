
CREATE VIEW Landing.MYSOLUTION_Customer_CustomerRole_MappingView
AS
WITH TableData
AS (
    SELECT
        Customer_Id,
        CustomerRole_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            CustomerRole_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            CustomerRole_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM MYSOLUTION.Customer_CustomerRole_Mapping
)
SELECT
    -- Chiavi
    TD.Customer_Id,
    TD.CustomerRole_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

    -- Attributi

FROM TableData TD;

GO

