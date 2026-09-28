CREATE TABLE [Landing].[MYSOLUTION_CustomerAddresses] (
    [Customer_Id]            INT            NOT NULL,
    [Address_Id]             INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_CustomerAddresses] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Customer_Id] ASC, [Address_Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_CustomerAddresses_BusinessKey]
    ON [Landing].[MYSOLUTION_CustomerAddresses]([Customer_Id] ASC, [Address_Id] ASC);


GO

