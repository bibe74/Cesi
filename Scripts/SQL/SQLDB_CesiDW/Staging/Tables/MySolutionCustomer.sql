CREATE TABLE [Staging].[MySolutionCustomer] (
    [Id]                       INT             NOT NULL,
    [HistoricalHashKey]        VARBINARY (20)  NULL,
    [ChangeHashKey]            VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]   VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]       VARCHAR (34)    NULL,
    [InsertDatetime]           DATETIME        NOT NULL,
    [UpdateDatetime]           DATETIME        NOT NULL,
    [IsDeleted]                BIT             NULL,
    [Username]                 NVARCHAR (1000) NULL,
    [Email]                    NVARCHAR (1000) NULL,
    [IdCometa]                 INT             NOT NULL,
    [Company]                  NVARCHAR (MAX)  NULL,
    [CodiceFiscale]            NVARCHAR (MAX)  NULL,
    [VATNumber]                NVARCHAR (MAX)  NULL,
    [FirstName]                NVARCHAR (MAX)  NULL,
    [LastName]                 NVARCHAR (MAX)  NULL,
    [StreetAddress]            NVARCHAR (MAX)  NULL,
    [ZipPostalCode]            NVARCHAR (MAX)  NULL,
    [Phone]                    NVARCHAR (MAX)  NULL,
    [Cellulare]                NVARCHAR (MAX)  NULL,
    [City]                     NVARCHAR (MAX)  NULL,
    [CountryId]                NVARCHAR (MAX)  NULL,
    [Country]                  NVARCHAR (100)  NULL,
    [StateProvinceId]          INT             NULL,
    [StateProvince]            NVARCHAR (100)  NULL,
    [rnCustomerDESC]           BIGINT          NULL,
    [HasRoleMySolutionDemo]    BIT             NOT NULL,
    [HasRoleMySolutionInterno] BIT             NOT NULL,
    [CreatedOnUtc]             DATE            NULL,
    [DateExpiration]           DATE            NULL,
    CONSTRAINT [PK_Staging_MySolutionCustomer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_MySolutionCustomer_BusinessKey]
    ON [Staging].[MySolutionCustomer]([Id] ASC);


GO

