CREATE TABLE [Landing].[MYSOLUTION_OrderItemPartecipant] (
    [OrderItemId]                     INT             NOT NULL,
    [PartecipantId]                   INT             NOT NULL,
    [HistoricalHashKey]               VARBINARY (20)  NULL,
    [ChangeHashKey]                   VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]          VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]              VARCHAR (34)    NULL,
    [InsertDatetime]                  DATETIME        NOT NULL,
    [UpdateDatetime]                  DATETIME        NOT NULL,
    [IsDeleted]                       BIT             NULL,
    [OrderId]                         INT             NOT NULL,
    [OrderTotal]                      NUMERIC (18, 4) NOT NULL,
    [OrderAuthorizationTransactionId] NVARCHAR (MAX)  NULL,
    [OrderPaidDate]                   DATETIME2 (7)   NULL,
    [OrderCreatedDate]                DATETIME2 (7)   NOT NULL,
    [OrderCustomerId]                 INT             NOT NULL,
    [OrderStatusId]                   INT             NOT NULL,
    [OrderNumber]                     NVARCHAR (MAX)  NOT NULL,
    [OrderItemUnitPriceExclTax]       NUMERIC (18, 4) NOT NULL,
    [OrderItemAttributeDescription]   NVARCHAR (MAX)  NULL,
    [AttributeMappingId]              VARCHAR (MAX)   NULL,
    [AttributeValue]                  VARCHAR (MAX)   NULL,
    [AttributeMappingId2]             VARCHAR (MAX)   NULL,
    [AttributeValue2]                 VARCHAR (MAX)   NULL,
    [ProductId]                       INT             NOT NULL,
    [ProductName]                     NVARCHAR (400)  NOT NULL,
    [ProductShortDescription]         NVARCHAR (MAX)  NULL,
    [ProductSku]                      NVARCHAR (400)  NULL,
    [ProductGtin]                     NVARCHAR (400)  NULL,
    [ProductSubdescription]           NVARCHAR (400)  NULL,
    [ProductAttributeCombinationSku]  NVARCHAR (400)  NULL,
    [ProductAttributeCombinationGtin] NVARCHAR (400)  NULL,
    [PartecipantFirstName]            NVARCHAR (MAX)  NULL,
    [PartecipantLastName]             NVARCHAR (MAX)  NULL,
    [PartecipantEmail]                NVARCHAR (MAX)  NULL,
    [PartecipantFiscalCode]           NVARCHAR (MAX)  NULL,
    [RootPartecipantEmail]            NVARCHAR (MAX)  NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_OrderItemPartecipant] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [OrderItemId] ASC, [PartecipantId] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_OrderItemPartecipant_BusinessKey]
    ON [Landing].[MYSOLUTION_OrderItemPartecipant]([OrderItemId] ASC, [PartecipantId] ASC);


GO

