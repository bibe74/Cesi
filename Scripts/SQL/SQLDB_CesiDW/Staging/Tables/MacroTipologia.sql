CREATE TABLE [Staging].[MacroTipologia] (
    [MacroTipologia]                NVARCHAR (60)  NOT NULL,
    [HistoricalHashKey]             VARBINARY (20) NULL,
    [ChangeHashKey]                 VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]        VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]            VARCHAR (34)   NULL,
    [InsertDatetime]                DATETIME       NOT NULL,
    [UpdateDatetime]                DATETIME       NOT NULL,
    [IsDeleted]                     BIT            NULL,
    [IsValidaPerBudgetNuoveVendite] BIT            NOT NULL,
    [IsValidaPerBudgetRinnovi]      BIT            NOT NULL,
    CONSTRAINT [PK_Import_MacroTipologia] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [MacroTipologia] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MacroTipologia_BusinessKey]
    ON [Staging].[MacroTipologia]([MacroTipologia] ASC);


GO

