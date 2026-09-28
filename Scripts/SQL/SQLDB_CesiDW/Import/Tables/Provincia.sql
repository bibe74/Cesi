CREATE TABLE [Import].[Provincia] (
    [Provincia]             NVARCHAR (50) NOT NULL,
    [CodSiglaProvincia]     NVARCHAR (50) NOT NULL,
    [DescrProvincia]        NVARCHAR (50) NOT NULL,
    [CodCittaMetropolitana] NVARCHAR (50) NOT NULL,
    [CodRegione]            NVARCHAR (50) NOT NULL,
    [DescrRegione]          NVARCHAR (50) NOT NULL,
    [CodMacroregione]       NVARCHAR (50) NOT NULL,
    [DescrMacroregione]     NVARCHAR (50) NOT NULL,
    [CodNazione]            NVARCHAR (50) NOT NULL,
    [DescrNazione]          NVARCHAR (50) NOT NULL,
    [DataInizioValidita]    NVARCHAR (50) NOT NULL,
    [DataFineValidita]      NVARCHAR (50) NOT NULL,
    CONSTRAINT [PK_Import_Provincia] PRIMARY KEY CLUSTERED ([CodSiglaProvincia] ASC)
);


GO

