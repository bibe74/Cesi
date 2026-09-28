CREATE TABLE [Import].[ProvinciaAgenteCapoArea] (
    [IDProvincia] NVARCHAR (10) NOT NULL,
    [Agente]      NVARCHAR (60) NOT NULL,
    [CapoArea]    NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Import_ProvinciaAgenteCapoArea] PRIMARY KEY CLUSTERED ([IDProvincia] ASC)
);


GO

