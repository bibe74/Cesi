CREATE TABLE [Import].[LiquidazioneProvvigioneTeorica] (
    [DurataContratto]                NVARCHAR (20) NOT NULL,
    [CodiceCondizioniPagamento]      NVARCHAR (10) NOT NULL,
    [LiquidazioneProvvigioneTeorica] NVARCHAR (40) NOT NULL,
    CONSTRAINT [PK_Import_LiquidazioneProvvigioneTeorica] PRIMARY KEY CLUSTERED ([DurataContratto] ASC, [CodiceCondizioniPagamento] ASC)
);


GO

