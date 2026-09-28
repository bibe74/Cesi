CREATE TABLE [Staging].[Accessi] (
    [PKData]                 DATE           NOT NULL,
    [PKCliente]              INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [PKCapoArea]             INT            NULL,
    [NumeroAccessi]          INT            NOT NULL,
    [NumeroPagineVisitate]   INT            NOT NULL,
    CONSTRAINT [PK_Staging_Accessi] PRIMARY KEY CLUSTERED ([PKData] ASC, [PKCliente] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_Accessi_PKCliente_PKData]
    ON [Staging].[Accessi]([PKCliente] ASC, [PKData] ASC);


GO

