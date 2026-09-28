CREATE TABLE [Staging].[DomandeMIA] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Testo]                  NVARCHAR (MAX) NULL,
    [IsDomanda]              BIT            NOT NULL,
    [PKDataCreazione]        DATE           NOT NULL,
    [Area]                   NVARCHAR (64)  NULL,
    [Email]                  NVARCHAR (256) NOT NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAIMessage] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAIMessage_BusinessKey]
    ON [Staging].[DomandeMIA]([Id] ASC);


GO

