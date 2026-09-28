CREATE TABLE [Landing].[COMETA_CategoriaCommercialeArticolo] (
    [id_cat_com_articolo]    INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (10)  NOT NULL,
    [descrizione]            NVARCHAR (40)  NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_CategoriaCommercialeArticolo] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_cat_com_articolo] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_CategoriaCommercialeArticolo_BusinessKey]
    ON [Landing].[COMETA_CategoriaCommercialeArticolo]([id_cat_com_articolo] ASC);


GO

