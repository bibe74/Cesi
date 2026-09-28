
CREATE PROCEDURE MYSOLUTION.usp_Merge_OrderItemPartecipant
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_OrderItemPartecipant AS TGT
    USING Landing.MYSOLUTION_OrderItemPartecipantView (nolock) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.PartecipantId = TGT.PartecipantId

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.OrderId = SRC.OrderId,
        TGT.OrderTotal = SRC.OrderTotal,
        TGT.OrderAuthorizationTransactionId = SRC.OrderAuthorizationTransactionId,
        TGT.OrderPaidDate = SRC.OrderPaidDate,
        TGT.OrderCreatedDate = SRC.OrderCreatedDate,
        TGT.OrderCustomerId = SRC.OrderCustomerId,
        TGT.OrderStatusId = SRC.OrderStatusId,
        TGT.OrderNumber = SRC.OrderNumber,
        TGT.OrderItemUnitPriceExclTax = SRC.OrderItemUnitPriceExclTax,
        TGT.OrderItemAttributeDescription = SRC.OrderItemAttributeDescription,
        TGT.AttributeMappingId = SRC.AttributeMappingId,
        TGT.AttributeValue = SRC.AttributeValue,
        TGT.AttributeMappingId2 = SRC.AttributeMappingId2,
        TGT.AttributeValue2 = SRC.AttributeValue2,
        TGT.ProductId = SRC.ProductId,
        TGT.ProductName = SRC.ProductName,
        TGT.ProductShortDescription = SRC.ProductShortDescription,
        TGT.ProductSku = SRC.ProductSku,
        TGT.ProductGtin = SRC.ProductGtin,
        TGT.ProductSubdescription = SRC.ProductSubdescription,
        TGT.ProductAttributeCombinationSku = SRC.ProductAttributeCombinationSku,
        TGT.ProductAttributeCombinationGtin = SRC.ProductAttributeCombinationGtin,
        TGT.PartecipantFirstName = SRC.PartecipantFirstName,
        TGT.PartecipantLastName = SRC.PartecipantLastName,
        TGT.PartecipantEmail = SRC.PartecipantEmail,
        TGT.PartecipantFiscalCode = SRC.PartecipantFiscalCode,
        TGT.RootPartecipantEmail = SRC.RootPartecipantEmail

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        OrderItemId,
        PartecipantId,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        OrderId,
        OrderTotal,
        OrderAuthorizationTransactionId,
        OrderPaidDate,
        OrderCreatedDate,
        OrderCustomerId,
        OrderStatusId,
        OrderNumber,
        OrderItemUnitPriceExclTax,
        OrderItemAttributeDescription,
        AttributeMappingId,
        AttributeValue,
        AttributeMappingId2,
        AttributeValue2,
        ProductId,
        ProductName,
        ProductShortDescription,
        ProductSku,
        ProductGtin,
        ProductSubdescription,
        ProductAttributeCombinationSku,
        ProductAttributeCombinationGtin,
        PartecipantFirstName,
        PartecipantLastName,
        PartecipantEmail,
        PartecipantFiscalCode,
        RootPartecipantEmail
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
        'Landing.MYSOLUTION_OrderItemPartecipant' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR) + ', PartecipantId = ' + CAST(COALESCE(inserted.PartecipantId, deleted.PartecipantId) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

