CREATE TABLE [Landing].[COMETA_Tipo_Fatturazione] (
    [id_tipo_fatturazione]   INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (10)  NULL,
    [descrizione]            NVARCHAR (200) NULL,
    CONSTRAINT [PK_Landing_COMETA_Tipo_Fatturazione] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_tipo_fatturazione] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Tipo_Fatturazione_BusinessKey]
    ON [Landing].[COMETA_Tipo_Fatturazione]([id_tipo_fatturazione] ASC);


GO

