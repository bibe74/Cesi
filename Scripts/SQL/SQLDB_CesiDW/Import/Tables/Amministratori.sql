CREATE TABLE [Import].[Amministratori] (
    [Amministratore] NVARCHAR (60)  NOT NULL,
    [ADUser]         NVARCHAR (60)  NOT NULL,
    [Email]          NVARCHAR (100) NOT NULL,
    CONSTRAINT [PK_Import_Amministratori] PRIMARY KEY CLUSTERED ([Amministratore] ASC)
);


GO

