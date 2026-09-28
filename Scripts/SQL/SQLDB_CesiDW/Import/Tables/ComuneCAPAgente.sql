CREATE TABLE [Import].[ComuneCAPAgente] (
    [IDProvincia] NVARCHAR (10) NOT NULL,
    [Comune]      NVARCHAR (60) NOT NULL,
    [CAP]         NVARCHAR (10) NOT NULL,
    [Agente]      NVARCHAR (60) NOT NULL,
    [CapoArea]    NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Import_ComuneCAPAgente] PRIMARY KEY CLUSTERED ([IDProvincia] ASC, [Comune] ASC, [CAP] ASC)
);


GO

