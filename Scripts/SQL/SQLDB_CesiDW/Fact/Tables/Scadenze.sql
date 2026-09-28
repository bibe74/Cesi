CREATE TABLE [Fact].[Scadenze] (
    [PKScadenze]             INT             CONSTRAINT [DFT_Fact_Scadenze_PKScadenze] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_Scadenze]) NOT NULL,
    [PKCliente]              INT             NOT NULL,
    [PKDataScadenza]         DATE            NOT NULL,
    [PKDocumenti]            INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NOT NULL,
    [IDScadenza]             INT             NOT NULL,
    [TipoScadenza]           CHAR (1)        NOT NULL,
    [IDSoggettoCommerciale]  INT             NOT NULL,
    [StatoScadenza]          CHAR (1)        NOT NULL,
    [EsitoPagamento]         CHAR (1)        NOT NULL,
    [IDDocumento]            INT             NOT NULL,
    [ImportoScadenza]        DECIMAL (10, 2) NOT NULL,
    [ImportoSaldato]         DECIMAL (10, 2) NOT NULL,
    [ImportoResiduo]         DECIMAL (10, 2) NOT NULL,
    CONSTRAINT [PK_Fact_Scadenze] PRIMARY KEY CLUSTERED ([PKScadenze] ASC),
    CONSTRAINT [FK_Fact_Scadenze_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_Scadenze_PKDataScadenza] FOREIGN KEY ([PKDataScadenza]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_Scadenze_PKDocumenti] FOREIGN KEY ([PKDocumenti]) REFERENCES [Fact].[Documenti] ([PKDocumenti])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_Scadenze_IDScadenza]
    ON [Fact].[Scadenze]([IDScadenza] ASC);


GO

