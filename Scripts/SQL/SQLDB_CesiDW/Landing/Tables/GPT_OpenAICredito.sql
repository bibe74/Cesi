CREATE TABLE [Landing].[GPT_OpenAICredito] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [ClienteId]              INT            NOT NULL,
    [CausaleId]              INT            NOT NULL,
    [PartitaId]              INT            NULL,
    [MessageId]              INT            NULL,
    [Documento]              NVARCHAR (MAX) NULL,
    [DataMovimento]          DATE           NOT NULL,
    [Quantita]               INT            NOT NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAICredito] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICredito_BusinessKey]
    ON [Landing].[GPT_OpenAICredito]([Id] ASC);


GO

