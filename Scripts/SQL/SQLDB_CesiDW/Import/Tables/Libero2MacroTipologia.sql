CREATE TABLE [Import].[Libero2MacroTipologia] (
    [IDLibero2]                     NVARCHAR (10) NOT NULL,
    [Libero2]                       NVARCHAR (60) NOT NULL,
    [MacroTipologia]                NVARCHAR (60) NOT NULL,
    [IsValidaPerBudgetNuoveVendite] BIT           NOT NULL,
    [IsValidaPerBudgetRinnovi]      BIT           NOT NULL,
    CONSTRAINT [PK_Import_Libero2MacroTipologia] PRIMARY KEY CLUSTERED ([IDLibero2] ASC)
);


GO

