CREATE TABLE [Landing].[COMETA_MovimentiScadenza] (
    [id_mov_scadenza]        INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [id_scadenza]            INT            NULL,
    [data]                   DATETIME2 (7)  NULL,
    [importo]                NUMERIC (18)   NULL,
    CONSTRAINT [PK_Landing_COMETA_MovimentiScadenza] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_mov_scadenza] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_MovimentiScadenza_BusinessKey]
    ON [Landing].[COMETA_MovimentiScadenza]([id_mov_scadenza] ASC);


GO

