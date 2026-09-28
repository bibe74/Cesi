CREATE TABLE [Landing].[COMETA_Articolo] (
    [id_articolo]            INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (40)  NOT NULL,
    [descrizione]            NVARCHAR (80)  NULL,
    [id_cat_com_articolo]    INT            NULL,
    [id_cat_merceologica]    INT            NULL,
    [des_breve]              NVARCHAR (80)  NULL,
    CONSTRAINT [PK_Landing_COMETA_Articolo] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_articolo] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Articolo_BusinessKey]
    ON [Landing].[COMETA_Articolo]([id_articolo] ASC);


GO

