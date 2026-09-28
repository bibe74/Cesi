CREATE TABLE [Landing].[GPT_OpenAIThread] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [ClienteId]              INT            NOT NULL,
    [Area]                   NVARCHAR (64)  NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAIThread] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAIThread_BusinessKey]
    ON [Landing].[GPT_OpenAIThread]([Id] ASC);


GO

