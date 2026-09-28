CREATE TABLE [Dim].[Articolo] (
    [PKArticolo]                  INT            CONSTRAINT [DFT_Dim_Articolo_PKArticolo] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_Articolo]) NOT NULL,
    [id_articolo]                 INT            NOT NULL,
    [HistoricalHashKey]           VARBINARY (20) NULL,
    [ChangeHashKey]               VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]      VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]          VARCHAR (34)   NULL,
    [InsertDatetime]              DATETIME       CONSTRAINT [DFT_Dim_Articolo_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]              DATETIME       CONSTRAINT [DFT_Dim_Articolo_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]                   BIT            CONSTRAINT [DFT_Dim_Articolo_IsDeleted] DEFAULT ((0)) NOT NULL,
    [Codice]                      NVARCHAR (80)  NOT NULL,
    [Descrizione]                 NVARCHAR (80)  NOT NULL,
    [CodiceCategoriaCommerciale]  NVARCHAR (10)  NOT NULL,
    [CategoriaCommerciale]        NVARCHAR (40)  NOT NULL,
    [CodiceCategoriaMerceologica] NVARCHAR (10)  NOT NULL,
    [CategoriaMerceologica]       NVARCHAR (40)  NOT NULL,
    [DescrizioneBreve]            NVARCHAR (80)  NOT NULL,
    [CodiceEsercizioMaster]       NVARCHAR (10)  NOT NULL,
    [CategoriaMaster]             NVARCHAR (40)  NOT NULL,
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
    CONSTRAINT [PK_Dim_Articolo] PRIMARY KEY CLUSTERED ([PKArticolo] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Articolo_id_articolo]
    ON [Dim].[Articolo]([id_articolo] ASC);


GO

