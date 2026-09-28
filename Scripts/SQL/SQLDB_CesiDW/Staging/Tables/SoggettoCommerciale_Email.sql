CREATE TABLE [Staging].[SoggettoCommerciale_Email] (
    [IDSoggettoCommerciale]     INT            NOT NULL,
    [Email]                     NVARCHAR (120) NOT NULL,
    [HistoricalHashKey]         VARBINARY (20) NULL,
    [ChangeHashKey]             VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)   NULL,
    [InsertDatetime]            DATETIME       NOT NULL,
    [UpdateDatetime]            DATETIME       NOT NULL,
    [IsDeleted]                 BIT            NULL,
    [rnEmail]                   INT            NOT NULL,
    [rnSoggettoCommercialeDESC] INT            NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_Telefono] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDSoggettoCommerciale] ASC, [Email] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [Staging_SoggettoCommerciale_IDSoggettoCommerciale_rnEmail]
    ON [Staging].[SoggettoCommerciale_Email]([IDSoggettoCommerciale] ASC, [rnEmail] ASC);


GO

CREATE UNIQUE NONCLUSTERED INDEX [Staging_SoggettoCommerciale_Email_Email_rnSoggettoCommercialeDESC]
    ON [Staging].[SoggettoCommerciale_Email]([Email] ASC, [rnSoggettoCommercialeDESC] ASC);


GO

CREATE UNIQUE NONCLUSTERED INDEX [Staging_SoggettoCommerciale_Email_BusinessKey]
    ON [Staging].[SoggettoCommerciale_Email]([IDSoggettoCommerciale] ASC, [Email] ASC);


GO

