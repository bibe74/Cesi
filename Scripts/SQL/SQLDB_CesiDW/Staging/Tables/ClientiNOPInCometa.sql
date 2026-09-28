CREATE TABLE [Staging].[ClientiNOPInCometa] (
    [PKClienteNOP]    INT            NOT NULL,
    [Email]           NVARCHAR (120) NOT NULL,
    [PKClienteCometa] INT            NOT NULL,
    CONSTRAINT [PK_Staging_ClientiNOPInCometa] PRIMARY KEY CLUSTERED ([PKClienteNOP] ASC)
);


GO

