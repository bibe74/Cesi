CREATE TABLE [Landing].[MYSOLUTION_Users] (
    [ID]                     INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [Email]                  NVARCHAR (1000) NULL,
    [RagioneSociale]         NVARCHAR (MAX)  NOT NULL,
    [Nome]                   NVARCHAR (MAX)  NOT NULL,
    [Cognome]                NVARCHAR (MAX)  NOT NULL,
    [Citta]                  NVARCHAR (MAX)  NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Users] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Users_BusinessKey]
    ON [Landing].[MYSOLUTION_Users]([ID] ASC);


GO

