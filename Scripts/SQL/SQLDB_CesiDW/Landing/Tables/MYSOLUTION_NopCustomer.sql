CREATE TABLE [Landing].[MYSOLUTION_NopCustomer] (
    [Id]                     INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [Username]               NVARCHAR (1000) NULL,
    [Email]                  NVARCHAR (1000) NULL,
    [Active]                 BIT             NOT NULL,
    [Deleted]                BIT             NOT NULL,
    [BillingAddress_Id]      INT             NULL,
    [ShippingAddress_Id]     INT             NULL,
    [IdCometa]               INT             NOT NULL,
    [DateExpiration]         DATETIME2 (7)   NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_NopCustomer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_NopCustomer_BusinessKey]
    ON [Landing].[MYSOLUTION_NopCustomer]([Id] ASC);


GO

