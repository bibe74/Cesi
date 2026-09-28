CREATE TABLE [Import].[Decod_Tipo_Reg] (
    [tipo_registro]  CHAR (2)      NOT NULL,
    [descr_tipo_reg] NVARCHAR (20) NOT NULL,
    CONSTRAINT [PK_Import_Decod_Tipo_Reg] PRIMARY KEY CLUSTERED ([tipo_registro] ASC)
);


GO

