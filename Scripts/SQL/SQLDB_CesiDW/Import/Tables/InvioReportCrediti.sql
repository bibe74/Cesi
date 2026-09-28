CREATE TABLE [Import].[InvioReportCrediti] (
    [Email]       NVARCHAR (40)  NOT NULL,
    [Descrizione] NVARCHAR (40)  NULL,
    [Note]        NVARCHAR (400) NULL,
    CONSTRAINT [PK_Import_InvioReportCrediti] PRIMARY KEY CLUSTERED ([Email] ASC)
);


GO

