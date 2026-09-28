CREATE TABLE [Fact].[CreditiOpenAIDettaglio] (
    [PKCreditiOpenAIDettaglio]    INT            CONSTRAINT [DFT_Fact_CreditiOpenAIDettaglio_PKCreditiOpenAIDettaglio] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_CreditiOpenAIDettaglio]) NOT NULL,
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
    [IsDeleted]                   BIT            NOT NULL,
    [PKDataInizioDemo]            DATE           NOT NULL,
    [QtaCreditiResidui]           INT            NULL,
    [QtaCreditiUtilizzati]        INT            NULL,
    [ConteggioDomandeERisposte]   INT            NULL,
    CONSTRAINT [PK_Fact_CreditiOpenAIDettaglio] PRIMARY KEY CLUSTERED ([PKCreditiOpenAIDettaglio] ASC),
    CONSTRAINT [FK_Fact_CreditiOpenAIDettaglio_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_CreditiOpenAIDettaglio_PKDataCreazionePartita] FOREIGN KEY ([PKDataCreazionePartita]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_CreditiOpenAIDettaglio_PKDataScadenzaPartita] FOREIGN KEY ([PKDataScadenzaPartita]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_CreditiOpenAIDettaglio_PKDataUltimoUtilizzo] FOREIGN KEY ([PKDataUltimoUtilizzo]) REFERENCES [Dim].[Data] ([PKData])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_CreditiOpenAIDettaglio_BusinessKey]
    ON [Fact].[CreditiOpenAIDettaglio]([PKCliente] ASC, [CodiceOrdine] ASC, [PKDataUltimoUtilizzo] ASC, [PKDataCreazionePartita] ASC, [PKDataScadenzaPartita] ASC, [QtaCreditiCaricatiInPartita] ASC);


GO

