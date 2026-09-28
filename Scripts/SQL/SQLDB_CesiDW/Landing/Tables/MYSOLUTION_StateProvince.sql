CREATE TABLE [Landing].[MYSOLUTION_StateProvince] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [CountryId]              INT            NOT NULL,
    [Name]                   NVARCHAR (100) NOT NULL,
    [Abbreviation]           NVARCHAR (100) NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_StateProvince] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_StateProvince_BusinessKey]
    ON [Landing].[MYSOLUTION_StateProvince]([Id] ASC);


GO

