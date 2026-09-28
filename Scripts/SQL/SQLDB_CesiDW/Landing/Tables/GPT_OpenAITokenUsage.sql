CREATE TABLE [Landing].[GPT_OpenAITokenUsage] (
    [Id]                     INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [email]                  NVARCHAR (320)  NOT NULL,
    [threadId]               NVARCHAR (200)  NOT NULL,
    [conversationId]         NVARCHAR (200)  NOT NULL,
    [createdOn]              DATETIME2 (7)   NOT NULL,
    [assistantArea]          NVARCHAR (100)  NOT NULL,
    [responseMode]           NVARCHAR (50)   NOT NULL,
    [model]                  NVARCHAR (100)  NOT NULL,
    [vectorStorageId]        NVARCHAR (200)  NULL,
    [input_tokens]           BIGINT          NULL,
    [output_tokens]          BIGINT          NULL,
    [reasoning_tokens]       BIGINT          NULL,
    [total_tokens]           BIGINT          NULL,
    [estimatedTotalCostUsd]  NUMERIC (18, 6) NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAITokenUsage] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Landing_GPT_OpenAITokenUsage_BusinessKey]
    ON [Landing].[GPT_OpenAITokenUsage]([Id] ASC);


GO

