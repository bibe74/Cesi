CREATE TABLE [Import].[ProfiliDocumento] (
    [id_prof_documento]                               INT           NOT NULL,
    [Profilo]                                         NVARCHAR (60) NULL,
    [IsProfiloValidoPerStatisticaFatturato]           BIT           NOT NULL,
    [IsProfiloValidoPerStatisticaFatturatoFormazione] BIT           NOT NULL,
    CONSTRAINT [PK_Import_ProfiliDocumento] PRIMARY KEY CLUSTERED ([id_prof_documento] ASC)
);


GO

