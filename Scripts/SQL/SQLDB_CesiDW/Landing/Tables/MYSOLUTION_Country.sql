CREATE TABLE [Landing].[MYSOLUTION_Country] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Name]                   NVARCHAR (100) NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Country] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Country_BusinessKey]
    ON [Landing].[MYSOLUTION_Country]([Id] ASC);


GO

