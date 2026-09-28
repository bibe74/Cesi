CREATE TABLE [Import].[CapiArea] (
    [CapoArea]           NVARCHAR (60)   NOT NULL,
    [Agente]             NVARCHAR (60)   NOT NULL,
    [ADUser]             NVARCHAR (60)   NOT NULL,
    [Email]              NVARCHAR (100)  NOT NULL,
    [InvioEmail]         BIT             NOT NULL,
    [AgenteBudget]       NVARCHAR (60)   NOT NULL,
    [Prefisso]           NVARCHAR (3)    NOT NULL,
    [ProvvigioneNuovo]   DECIMAL (18, 2) NULL,
    [ProvvigioneRinnovo] DECIMAL (18, 2) NULL,
    CONSTRAINT [PK_Import_CapiArea] PRIMARY KEY CLUSTERED ([CapoArea] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Import_CapiArea_Prefisso]
    ON [Import].[CapiArea]([Prefisso] ASC);


GO

