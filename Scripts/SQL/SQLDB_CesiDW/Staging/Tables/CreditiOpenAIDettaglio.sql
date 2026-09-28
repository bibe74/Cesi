CREATE TABLE [Staging].[CreditiOpenAIDettaglio] (
    [PKCliente]                   INT            NOT NULL,
    [CodiceOrdine]                NVARCHAR (20)  NOT NULL,
    [PKDataUltimoUtilizzo]        DATE           NOT NULL,
    [PKDataCreazionePartita]      DATE           NOT NULL,
    [PKDataScadenzaPartita]       DATE           NOT NULL,
    [QtaCreditiCaricatiInPartita] INT            NOT NULL,
    [HistoricalHashKey]           VARBINARY (20) NULL,
    [ChangeHashKey]               VARBINARY (20) NULL,
    [InsertDatetime]              DATETIME       NOT NULL,
    [UpdateDatetime]              DATETIME       NOT NULL,
    [PKDataInizioDemo]            DATE           NOT NULL,
    [QtaCreditiResidui]           INT            NULL,
    [QtaCreditiUtilizzati]        INT            NULL,
    [ConteggioDomandeERisposte]   INT            NULL,
    CONSTRAINT [PK_Staging_CreditiOpenAIDettaglio] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [PKCliente] ASC, [CodiceOrdine] ASC, [PKDataUltimoUtilizzo] ASC, [PKDataCreazionePartita] ASC, [PKDataScadenzaPartita] ASC, [QtaCreditiCaricatiInPartita] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_CreditiOpenAI_BusinessKey]
    ON [Staging].[CreditiOpenAIDettaglio]([PKCliente] ASC, [CodiceOrdine] ASC, [PKDataUltimoUtilizzo] ASC, [PKDataCreazionePartita] ASC, [PKDataScadenzaPartita] ASC, [QtaCreditiCaricatiInPartita] ASC);


GO

