CREATE TABLE [Landing].[MYSOLUTION_Demo] (
    [Id]                     INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (32)  NULL,
    [ChangeHashKey]          VARBINARY (32)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [Email]                  NVARCHAR (1000) NULL,
    [RagioneSociale]         NVARCHAR (1000) NOT NULL,
    [Nome]                   NVARCHAR (1000) NOT NULL,
    [Cognome]                NVARCHAR (1000) NOT NULL,
    [Citta]                  NVARCHAR (1000) NOT NULL,
    [Provincia]              NVARCHAR (100)  NOT NULL,
    [SiglaProvincia]         NVARCHAR (100)  NOT NULL,
    [DataInizioDemo]         DATE            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Demo] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Demo_BusinessKey]
    ON [Landing].[MYSOLUTION_Demo]([Id] ASC);


GO

