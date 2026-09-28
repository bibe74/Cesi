CREATE TABLE [Import].[ProvinciaAgente] (
    [IDProvincia] NVARCHAR (10) NOT NULL,
    [Agente]      NVARCHAR (60) NOT NULL,
    [CapoArea]    NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Import_ProvinciaAgente] PRIMARY KEY CLUSTERED ([IDProvincia] ASC)
);


GO

