CREATE TABLE [Landing].[MYSOLUTION_CustomerRole] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Name]                   NVARCHAR (255) NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_CustomerRole] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_CustomerRole_BusinessKey]
    ON [Landing].[MYSOLUTION_CustomerRole]([Id] ASC);


GO

