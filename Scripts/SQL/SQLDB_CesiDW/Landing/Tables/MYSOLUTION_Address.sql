CREATE TABLE [Landing].[MYSOLUTION_Address] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [FirstName]              NVARCHAR (MAX) NULL,
    [LastName]               NVARCHAR (MAX) NULL,
    [Email]                  NVARCHAR (MAX) NULL,
    [Company]                NVARCHAR (MAX) NULL,
    [Country]                NVARCHAR (100) NULL,
    [StateProvince]          NVARCHAR (100) NULL,
    [City]                   NVARCHAR (MAX) NULL,
    [Address1]               NVARCHAR (MAX) NULL,
    [Address2]               NVARCHAR (MAX) NULL,
    [ZipPostalCode]          NVARCHAR (MAX) NULL,
    [PhoneNumber]            NVARCHAR (MAX) NULL,
    [County]                 NVARCHAR (MAX) NULL,
    [CodiceFiscale]          NVARCHAR (MAX) NULL,
    [Piva]                   NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Address] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Address_BusinessKey]
    ON [Landing].[MYSOLUTION_Address]([Id] ASC);


GO

