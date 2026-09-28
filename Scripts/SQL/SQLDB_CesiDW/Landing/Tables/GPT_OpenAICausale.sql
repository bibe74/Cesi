CREATE TABLE [Landing].[GPT_OpenAICausale] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Codice]                 NVARCHAR (32)  NOT NULL,
    [Descrizione]            NVARCHAR (128) NOT NULL,
    [Segno]                  SMALLINT       NOT NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAICausale] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICausale_AlternateKey]
    ON [Landing].[GPT_OpenAICausale]([Codice] ASC);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICausale_BusinessKey]
    ON [Landing].[GPT_OpenAICausale]([Id] ASC);


GO

