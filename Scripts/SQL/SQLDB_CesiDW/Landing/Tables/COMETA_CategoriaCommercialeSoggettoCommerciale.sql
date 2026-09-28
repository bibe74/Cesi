CREATE TABLE [Landing].[COMETA_CategoriaCommercialeSoggettoCommerciale] (
    [id_cat_com_sc]          INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (60)  NULL,
    [descrizione]            NVARCHAR (60)  NULL,
    CONSTRAINT [PK_Landing_COMETA_CategoriaCommercialeSoggettoCommerciale] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_cat_com_sc] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_CategoriaCommercialeSoggettoCommerciale_BusinessKey]
    ON [Landing].[COMETA_CategoriaCommercialeSoggettoCommerciale]([id_cat_com_sc] ASC);


GO

