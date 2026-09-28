CREATE TABLE [Landing].[COMETA_SoggettoCommerciale] (
    [id_sog_commerciale]          INT            NOT NULL,
    [HistoricalHashKey]           VARBINARY (32) NULL,
    [ChangeHashKey]               VARBINARY (32) NULL,
    [HistoricalHashKeyASCII]      VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]          VARCHAR (34)   NULL,
    [InsertDatetime]              DATETIME       NOT NULL,
    [UpdateDatetime]              DATETIME       NOT NULL,
    [IsDeleted]                   BIT            NULL,
    [codice]                      NVARCHAR (10)  NULL,
    [id_anagrafica]               INT            NULL,
    [tipo]                        CHAR (1)       NULL,
    [id_gruppo_agenti]            INT            NULL,
    [id_cat_com_sc]               INT            NULL,
    [rnIDSoggettoCommercialeDESC] INT            NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_SoggettoCommerciale] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_sog_commerciale] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_SoggettoCommerciale_BusinessKey]
    ON [Landing].[COMETA_SoggettoCommerciale]([id_sog_commerciale] ASC);


GO

