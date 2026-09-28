CREATE TABLE [Dim].[MacroTipologia] (
    [PKMacroTipologia]              INT            CONSTRAINT [DFT_Dim_MacroTipologia_PKMacroTipologia] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_MacroTipologia]) NOT NULL,
    [MacroTipologia]                NVARCHAR (60)  NOT NULL,
    [HistoricalHashKey]             VARBINARY (20) NULL,
    [ChangeHashKey]                 VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]        VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]            VARCHAR (34)   NULL,
    [InsertDatetime]                DATETIME       CONSTRAINT [DFT_Dim_MacroTipologia_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]                DATETIME       CONSTRAINT [DFT_Dim_MacroTipologia_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]                     BIT            CONSTRAINT [DFT_Dim_MacroTipologia_IsDeleted] DEFAULT ((0)) NOT NULL,
    [IsValidaPerBudgetNuoveVendite] BIT            NOT NULL,
    [IsValidaPerBudgetRinnovi]      BIT            NOT NULL,
    CONSTRAINT [PK_Dim_MacroTipologia] PRIMARY KEY CLUSTERED ([PKMacroTipologia] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_MacroTipologia_id_MacroTipologia]
    ON [Dim].[MacroTipologia]([MacroTipologia] ASC);


GO

