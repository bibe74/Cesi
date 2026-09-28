CREATE TABLE [Landing].[GPT_OpenAIMessage] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [OpenAIThreadId]         INT            NOT NULL,
    [Message]                NVARCHAR (MAX) NULL,
    [IsQuestion]             BIT            NOT NULL,
    [CreatedOn]              SMALLDATETIME  NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAIMessage] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAIMessage_BusinessKey]
    ON [Landing].[GPT_OpenAIMessage]([Id] ASC);


GO

