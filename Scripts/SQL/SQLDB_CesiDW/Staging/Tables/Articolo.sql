CREATE TABLE [Staging].[Articolo] (
    [id_articolo]                 INT            NOT NULL,
    [HistoricalHashKey]           VARBINARY (20) NULL,
    [ChangeHashKey]               VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]      VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]          VARCHAR (34)   NULL,
    [InsertDatetime]              DATETIME       NOT NULL,
    [UpdateDatetime]              DATETIME       NOT NULL,
    [IsDeleted]                   BIT            NULL,
    [Codice]                      NVARCHAR (40)  NOT NULL,
    [Descrizione]                 NVARCHAR (80)  NOT NULL,
    [CodiceCategoriaCommerciale]  NVARCHAR (10)  NOT NULL,
    [CategoriaCommerciale]        NVARCHAR (40)  NOT NULL,
    [CodiceCategoriaMerceologica] NVARCHAR (10)  NOT NULL,
    [CategoriaMerceologica]       NVARCHAR (40)  NOT NULL,
    [DescrizioneBreve]            NVARCHAR (80)  NOT NULL,
    [CategoriaMaster]             NVARCHAR (40)  NOT NULL,
    [CodiceEsercizioMaster]       NVARCHAR (10)  NOT NULL,
    [Fatturazione]                NVARCHAR (20)  NOT NULL,
    [Tipo]                        NVARCHAR (20)  NOT NULL,
    [Data1]                       NVARCHAR (40)  NOT NULL,
    [Data2]                       NVARCHAR (40)  NOT NULL,
    [Data3]                       NVARCHAR (40)  NOT NULL,
    [Data4]                       NVARCHAR (40)  NOT NULL,
    [Data5]                       NVARCHAR (40)  NOT NULL,
    [Data6]                       NVARCHAR (40)  NOT NULL,
    [LivelloMIA]                  NVARCHAR (1)   NOT NULL,
    [MacroTipoAbbonamento]        NVARCHAR (3)   NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_Articolo] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_articolo] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Articolo_BusinessKey]
    ON [Staging].[Articolo]([id_articolo] ASC);


GO

