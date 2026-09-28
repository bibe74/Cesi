CREATE TABLE [Landing].[GPT_OpenAICliente] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Email]                  NVARCHAR (256) NOT NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAICliente] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICliente_BusinessKey]
    ON [Landing].[GPT_OpenAICliente]([Id] ASC);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICliente_AlternateKey]
    ON [Landing].[GPT_OpenAICliente]([Email] ASC);


GO

