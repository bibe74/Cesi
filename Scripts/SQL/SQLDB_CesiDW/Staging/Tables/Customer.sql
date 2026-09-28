CREATE TABLE [Staging].[Customer] (
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
    [IdCometa]               INT             NOT NULL,
    [Company]                NVARCHAR (MAX)  NULL,
    [CodiceFiscale]          NVARCHAR (MAX)  NULL,
    [VATNumber]              NVARCHAR (MAX)  NULL,
    [FirstName]              NVARCHAR (MAX)  NULL,
    [LastName]               NVARCHAR (MAX)  NULL,
    [StreetAddress]          NVARCHAR (MAX)  NULL,
    [ZipPostalCode]          NVARCHAR (MAX)  NULL,
    [Phone]                  NVARCHAR (MAX)  NULL,
    [Cellulare]              NVARCHAR (MAX)  NULL,
    [City]                   NVARCHAR (MAX)  NULL,
    [CountryId]              NVARCHAR (MAX)  NULL,
    [Country]                NVARCHAR (100)  NULL,
    [StateProvinceId]        NVARCHAR (MAX)  NULL,
    [StateProvince]          NVARCHAR (100)  NULL,
    [rnCustomerDESC]         BIGINT          NULL,
    [HasRoleMySolutionDemo]  BIT             NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Customer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Customer_BusinessKey]
    ON [Staging].[Customer]([Id] ASC);


GO

