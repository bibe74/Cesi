CREATE TABLE [Landing].[GPT_OpenAIPartita] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Codice]                 NVARCHAR (128) NOT NULL,
    [Descrizione]            NVARCHAR (MAX) NOT NULL,
    [DataCreazione]          DATE           NOT NULL,
    [DataScadenza]           DATE           NOT NULL,
    [ClienteId]              INT            NOT NULL,
    [Stato]                  BIT            NOT NULL,
    [Quantita]               INT            NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAIPartita] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAIPartita_BusinessKey]
    ON [Landing].[GPT_OpenAIPartita]([Id] ASC);


GO

