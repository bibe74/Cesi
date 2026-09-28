
CREATE VIEW Landing.MYSOLUTION_OrderItemPartecipantView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            OI.Id,
            PA.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            O.Id,
            O.OrderTotal,
            O.AuthorizationTransactionId,
            O.PaidDateUtc,
            O.CreatedOnUtc,
            O.CustomerId,
            O.OrderStatusId,
            O.CustomOrderNumber,
            OI.UnitPriceExclTax,
            OI.AttributeDescription,
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[1]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[1]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[2]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[2]', 'varchar(max)')),
            P.Id,
            P.Name,
            P.ShortDescription,
            P.Sku,
            P.Gtin,
            P.Subdescription,
            PAC.Sku,
            PAC.Gtin,
            PA.FirstName,
            PA.LastName,
            PA.Email,
            PA.Ssn,
            PARoot.Email,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        O.Id AS OrderId,
        O.OrderTotal,
        O.AuthorizationTransactionId AS OrderAuthorizationTransactionId,
        O.PaidDateUtc AS OrderPaidDate,
        O.CreatedOnUtc AS OrderCreatedDate,
        O.CustomerId AS OrderCustomerId,
        O.OrderStatusId,
        O.CustomOrderNumber AS OrderNumber,

        OI.Id AS OrderItemId,
        OI.UnitPriceExclTax AS OrderItemUnitPriceExclTax,
        OI.AttributeDescription AS OrderItemAttributeDescription,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[1]', 'varchar(max)')) AS AttributeMappingId,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[1]', 'varchar(max)')) AS AttributeValue,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[2]', 'varchar(max)')) AS AttributeMappingId2,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[2]', 'varchar(max)')) AS AttributeValue2,

        P.Id AS ProductId,
        P.Name AS ProductName,
        P.ShortDescription AS ProductShortDescription,
        P.Sku AS ProductSku,
        P.Gtin AS ProductGtin,
        P.Subdescription AS ProductSubdescription,

        PAC.Sku AS ProductAttributeCombinationSku,
        PAC.Gtin AS ProductAttributeCombinationGtin,

        PA.Id AS PartecipantId,
        PA.FirstName AS PartecipantFirstName,
        PA.LastName AS PartecipantLastName,
        PA.Email AS PartecipantEmail,
        PA.Ssn AS PartecipantFiscalCode,
        PARoot.Email AS RootPartecipantEmail

    FROM MYSOLUTION.[Order] O
    INNER JOIN Landing.MYSOLUTION_Customer C ON C.Id = O.CustomerId
    INNER JOIN MYSOLUTION.OrderItem OI ON OI.OrderId = O.Id
    INNER JOIN MYSOLUTION.Product P ON P.Id = OI.ProductId
        AND P.Deleted = 0
    LEFT JOIN MYSOLUTION.ProductAttributeCombination PAC ON PAC.ProductId = OI.ProductId AND PAC.AttributesXml = OI.AttributesXml
    INNER JOIN MYSOLUTION.OrderItem_Partecipants OIP ON OIP.OrderItem_Id = OI.Id
    INNER JOIN MYSOLUTION.Partecipant PA ON PA.Id = OIP.Partecipant_Id
    INNER JOIN MYSOLUTION.Partecipant PARoot ON PARoot.Id = PA.OriginalPartecipantId
    WHERE O.OrderStatusId = 30 -- pagato
        AND O.Deleted = 0

)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.PartecipantId,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.OrderId,
    TD.OrderTotal,
    TD.OrderAuthorizationTransactionId,
    TD.OrderPaidDate,
    TD.OrderCreatedDate,
    TD.OrderCustomerId,
    TD.OrderStatusId,
    TD.OrderNumber,
    TD.OrderItemUnitPriceExclTax,
    TD.OrderItemAttributeDescription,
    TD.AttributeMappingId,
    TD.AttributeValue,
    TD.AttributeMappingId2,
    TD.AttributeValue2,
    TD.ProductId,
    TD.ProductName, -- CourseName
    TD.ProductShortDescription, -- CourseType
    TD.ProductSku, -- CourseCode
    TD.ProductGtin, -- WebinarCode
    TD.ProductSubdescription, -- StartDateDescription (dal dd/mm/yyyy)
    TD.ProductAttributeCombinationSku, -- AttCourseCode
    TD.ProductAttributeCombinationGtin, -- AttWebinarCode
    TD.PartecipantFirstName,
    TD.PartecipantLastName,
    TD.PartecipantEmail,
    TD.PartecipantFiscalCode,
    TD.RootPartecipantEmail

FROM TableData TD;

GO

