CREATE TABLE [Import].[TrascodificaGruppoAgenti] (
    [PKDataInizio]      DATE          NOT NULL,
    [PKDataFine]        DATE          NOT NULL,
    [PKGruppoAgenti]    INT           NOT NULL,
    [IDGruppoAgenti]    NVARCHAR (10) NOT NULL,
    [GruppoAgenti]      NVARCHAR (60) NOT NULL,
    [CapoArea]          NVARCHAR (60) NOT NULL,
    [PKGruppoAgentiNew] INT           NOT NULL,
    [IDGruppoAgentiNew] NVARCHAR (10) NOT NULL,
    [GruppoAgentiNew]   NVARCHAR (60) NOT NULL,
    [CapoAreaNew]       NVARCHAR (60) NOT NULL
);
GO

ALTER TABLE [Import].[TrascodificaGruppoAgenti]
    ADD CONSTRAINT [PK_Import_TrascodificaGruppoAgenti] PRIMARY KEY CLUSTERED ([PKDataInizio] ASC, [PKDataFine] ASC, [PKGruppoAgenti] ASC);
GO

