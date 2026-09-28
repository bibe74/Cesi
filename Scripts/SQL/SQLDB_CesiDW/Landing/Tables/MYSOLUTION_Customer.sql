CREATE TABLE [Landing].[MYSOLUTION_Customer] (
    [Id]                          INT              NOT NULL,
    [HistoricalHashKey]           VARBINARY (20)   NULL,
    [ChangeHashKey]               VARBINARY (20)   NULL,
    [HistoricalHashKeyASCII]      VARCHAR (34)     NULL,
    [ChangeHashKeyASCII]          VARCHAR (34)     NULL,
    [InsertDatetime]              DATETIME         NOT NULL,
    [UpdateDatetime]              DATETIME         NOT NULL,
    [IsDeleted]                   BIT              NULL,
    [Username]                    NVARCHAR (1000)  NULL,
    [Email]                       NVARCHAR (1000)  NULL,
    [IdCometa]                    INT              NOT NULL,
    [AdminComment]                NVARCHAR (MAX)   NULL,
    [IsTaxExempt]                 BIT              NOT NULL,
    [HasShoppingCartItems]        BIT              NOT NULL,
    [Active]                      BIT              NOT NULL,
    [Deleted]                     BIT              NOT NULL,
    [IsSystemAccount]             BIT              NOT NULL,
    [SystemName]                  NVARCHAR (400)   NULL,
    [LastIpAddress]               NVARCHAR (MAX)   NULL,
    [CreatedOnUtc]                DATETIME2 (7)    NOT NULL,
    [LastLoginDateUtc]            DATETIME2 (7)    NULL,
    [LastActivityDateUtc]         DATETIME2 (7)    NOT NULL,
    [CustomerGuid]                UNIQUEIDENTIFIER NOT NULL,
    [EmailToRevalidate]           NVARCHAR (1000)  NULL,
    [AffiliateId]                 INT              NOT NULL,
    [VendorId]                    INT              NOT NULL,
    [RequireReLogin]              BIT              NOT NULL,
    [FailedLoginAttempts]         INT              NOT NULL,
    [CannotLoginUntilDateUtc]     DATETIME2 (7)    NULL,
    [RegisteredInStoreId]         INT              NOT NULL,
    [BillingAddress_Id]           INT              NULL,
    [ShippingAddress_Id]          INT              NULL,
    [MysolutionSubscriptionQuote] INT              NULL,
    [SendRiqualification]         BIT              NOT NULL,
    [IsSpecial]                   BIT              NOT NULL,
    [DateExpiration]              DATETIME2 (7)    NULL,
    [StateProvinceId]             INT              NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Customer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Customer_BusinessKey]
    ON [Landing].[MYSOLUTION_Customer]([Id] ASC);


GO

