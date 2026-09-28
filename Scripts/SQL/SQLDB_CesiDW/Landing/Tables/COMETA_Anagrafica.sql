CREATE TABLE [Landing].[COMETA_Anagrafica] (
    [id_anagrafica]          INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [rag_soc_1]              NVARCHAR (60)  NULL,
    [rag_soc_2]              NVARCHAR (60)  NULL,
    [indirizzo]              NVARCHAR (60)  NULL,
    [cap]                    NVARCHAR (10)  NULL,
    [localita]               NVARCHAR (60)  NULL,
    [provincia]              NVARCHAR (10)  NULL,
    [nazione]                NVARCHAR (60)  NULL,
    [cod_fiscale]            NVARCHAR (60)  NULL,
    [par_iva]                NVARCHAR (60)  NULL,
    [indirizzo2]             NVARCHAR (60)  NULL,
    CONSTRAINT [PK_Landing_COMETA_Anagrafica] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_anagrafica] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Anagrafica_BusinessKey]
    ON [Landing].[COMETA_Anagrafica]([id_anagrafica] ASC);


GO

