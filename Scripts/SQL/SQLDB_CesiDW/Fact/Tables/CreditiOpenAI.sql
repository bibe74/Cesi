CREATE TABLE [Fact].[CreditiOpenAI] (
    [PKCreditiOpenAI]        INT            CONSTRAINT [DFT_Fact_CreditiOpenAI_PKCreditiOpenAI] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_CreditiOpenAI]) NOT NULL,
    [PKCliente]              INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NOT NULL,
    [PKDataScadenzaOrdine]   DATE           NOT NULL,
    [CreditiAcquistati]      INT            NULL,
    [CreditiConsumati]       INT            NULL,
    [CreditiFuoriOrdine]     INT            NULL,
    [CreditiResidui]         INT            NULL,
    CONSTRAINT [PK_Fact_CreditiOpenAI] PRIMARY KEY CLUSTERED ([PKCreditiOpenAI] ASC),
    CONSTRAINT [FK_Fact_CreditiOpenAI_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_CreditiOpenAI_PKDataScadenzaOrdine] FOREIGN KEY ([PKDataScadenzaOrdine]) REFERENCES [Dim].[Data] ([PKData])
);


GO

