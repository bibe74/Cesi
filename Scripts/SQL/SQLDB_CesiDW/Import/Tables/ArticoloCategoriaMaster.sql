CREATE TABLE [Import].[ArticoloCategoriaMaster] (
    [id_articolo]           INT            NOT NULL,
    [Codice]                NVARCHAR (40)  NOT NULL,
    [Descrizione]           NVARCHAR (80)  NULL,
    [CategoriaMaster]       NVARCHAR (40)  NOT NULL,
    [CodiceEsercizioMaster] NVARCHAR (10)  NOT NULL,
    [Percentuale]           DECIMAL (5, 2) NOT NULL,
    CONSTRAINT [PK_Import_ArticoloCategoriaMaster] PRIMARY KEY CLUSTERED ([id_articolo] ASC, [CategoriaMaster] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_Import_ArticoloCategoriaMaster_Codice]
    ON [Import].[ArticoloCategoriaMaster]([Codice] ASC);


GO

