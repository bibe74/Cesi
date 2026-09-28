CREATE TABLE [Landing].[COMETA_Scadenza] (
    [id_scadenza]            INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [tipo_scadenza]          NVARCHAR (MAX) NULL,
    [id_sog_commerciale]     INT            NULL,
    [data_scadenza]          DATETIME2 (7)  NULL,
    [importo]                NUMERIC (18)   NULL,
    [stato_scadenza]         NVARCHAR (MAX) NULL,
    [esito_pagamento]        NVARCHAR (MAX) NULL,
    [id_documento]           INT            NULL,
    CONSTRAINT [PK_Landing_COMETA_Scadenza] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_scadenza] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Scadenza_BusinessKey]
    ON [Landing].[COMETA_Scadenza]([id_scadenza] ASC);


GO

