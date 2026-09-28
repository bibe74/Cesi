CREATE TABLE [Staging].[Utente] (
    [IDUtente]               INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Email]                  NVARCHAR (60)  NOT NULL,
    [RagioneSociale]         NVARCHAR (120) NOT NULL,
    [Nome]                   NVARCHAR (60)  NOT NULL,
    [Cognome]                NVARCHAR (60)  NOT NULL,
    [Citta]                  NVARCHAR (60)  NOT NULL,
    [PKCliente]              INT            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Users] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDUtente] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Users_BusinessKey]
    ON [Staging].[Utente]([IDUtente] ASC);


GO

