CREATE TABLE [Import].[CapoAreaDefault_GruppoAgenti] (
    [CapoAreaDefault]       NVARCHAR (60) NOT NULL,
    [PKGruppoAgentiDefault] INT           NOT NULL,
    [IDGruppoAgenti]        NVARCHAR (10) NULL,
    [GruppoAgenti]          NVARCHAR (60) NULL,
    CONSTRAINT [PK_Import_CapoAreaDefault_GruppoAgenti] PRIMARY KEY CLUSTERED ([CapoAreaDefault] ASC)
);


GO

