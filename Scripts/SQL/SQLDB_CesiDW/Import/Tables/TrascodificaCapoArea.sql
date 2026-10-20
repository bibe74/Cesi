CREATE TABLE [Import].[TrascodificaCapoArea] (
    [PKDataInizio]  DATE          NOT NULL,
    [PKDataFine]    DATE          NOT NULL,
    [PKCapoArea]    INT           NOT NULL,
    [CapoArea]      NVARCHAR (60) NOT NULL,
    [PKCapoAreaNew] INT           NOT NULL,
    [CapoAreaNew]   NVARCHAR (60) NOT NULL
);
GO

ALTER TABLE [Import].[TrascodificaCapoArea]
    ADD CONSTRAINT [PK_Import_TrascodificaCapoArea] PRIMARY KEY CLUSTERED ([PKDataInizio] ASC, [PKDataFine] ASC, [PKCapoArea] ASC);
GO

