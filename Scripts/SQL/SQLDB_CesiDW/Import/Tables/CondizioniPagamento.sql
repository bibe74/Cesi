CREATE TABLE [Import].[CondizioniPagamento] (
    [CodiceCondizioniPagamento] NVARCHAR (10) NOT NULL,
    [CondizioniPagamento]       NVARCHAR (60) NOT NULL,
    [TipoContratto]             NVARCHAR (20) NOT NULL,
    [ProvvigioniAgenti]         NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Import_CondizioniPagamento] PRIMARY KEY CLUSTERED ([CodiceCondizioniPagamento] ASC)
);


GO

