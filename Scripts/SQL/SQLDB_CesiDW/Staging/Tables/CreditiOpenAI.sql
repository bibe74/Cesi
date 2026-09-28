CREATE TABLE [Staging].[CreditiOpenAI] (
    [PKCliente]              INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [PKDataScadenzaOrdine]   DATE           NOT NULL,
    [CreditiAcquistati]      INT            NULL,
    [CreditiConsumati]       INT            NULL,
    [CreditiFuoriOrdine]     INT            NULL,
    [CreditiResidui]         INT            NULL,
    CONSTRAINT [PK_Landing_GPT_OpenAICredito] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [PKCliente] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_GPT_OpenAICredito_BusinessKey]
    ON [Staging].[CreditiOpenAI]([PKCliente] ASC);


GO

