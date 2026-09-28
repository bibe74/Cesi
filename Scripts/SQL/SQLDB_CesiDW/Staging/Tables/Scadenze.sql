CREATE TABLE [Staging].[Scadenze] (
    [IDScadenza]             INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [TipoScadenza]           CHAR (1)        NOT NULL,
    [IDSoggettoCommerciale]  INT             NOT NULL,
    [PKCliente]              INT             NOT NULL,
    [PKDataScadenza]         DATE            NOT NULL,
    [StatoScadenza]          CHAR (1)        NOT NULL,
    [EsitoPagamento]         CHAR (1)        NOT NULL,
    [IDDocumento]            INT             NOT NULL,
    [PKDocumenti]            INT             NOT NULL,
    [ImportoScadenza]        DECIMAL (10, 2) NOT NULL,
    [ImportoSaldato]         DECIMAL (10, 2) NOT NULL,
    [ImportoResiduo]         DECIMAL (10, 2) NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_Scadenza] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDScadenza] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Scadenza_BusinessKey]
    ON [Staging].[Scadenze]([IDScadenza] ASC);


GO

