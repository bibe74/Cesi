CREATE TABLE [Staging].[AccessiCustomer] (
    [Username]               NVARCHAR (60)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [IDSoggettoCommerciale]  INT            NULL,
    [IDMySolution]           INT            NULL,
    CONSTRAINT [PK_Staging_AccessiCustomer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Username] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_AccessiCustomer_BusinessKey]
    ON [Staging].[AccessiCustomer]([Username] ASC);


GO

