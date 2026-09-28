CREATE TABLE [Landing].[MYSOLUTION_Customer_CustomerRole_Mapping] (
    [Customer_Id]            INT            NOT NULL,
    [CustomerRole_Id]        INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Customer_CustomerRole_Mapping] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Customer_Id] ASC, [CustomerRole_Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Customer_CustomerRole_Mapping_BusinessKey]
    ON [Landing].[MYSOLUTION_Customer_CustomerRole_Mapping]([Customer_Id] ASC, [CustomerRole_Id] ASC);


GO

