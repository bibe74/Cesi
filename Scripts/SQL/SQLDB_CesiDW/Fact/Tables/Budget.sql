CREATE TABLE [Fact].[Budget] (
    [PKBudget]                  INT             CONSTRAINT [DFT_Fact_Budget_PKBudget] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_Budget]) NOT NULL,
    [PKData]                    DATE            NOT NULL,
    [PKCapoArea]                INT             NOT NULL,
    [PKMacroTipologia]          INT             NOT NULL,
    [HistoricalHashKey]         VARBINARY (20)  NULL,
    [ChangeHashKey]             VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)    NULL,
    [InsertDatetime]            DATETIME        NOT NULL,
    [UpdateDatetime]            DATETIME        NOT NULL,
    [IsDeleted]                 BIT             NOT NULL,
    [ImportoBudgetNuoveVendite] DECIMAL (10, 2) NULL,
    [ImportoBudgetRinnovi]      DECIMAL (10, 2) NULL,
    [ImportoBudget]             DECIMAL (10, 2) NULL,
    CONSTRAINT [PK_Fact_Budget] PRIMARY KEY CLUSTERED ([PKBudget] ASC),
    CONSTRAINT [FK_Fact_Budget_PKCapoArea] FOREIGN KEY ([PKCapoArea]) REFERENCES [Dim].[CapoArea] ([PKCapoArea]),
    CONSTRAINT [FK_Fact_Budget_PKData] FOREIGN KEY ([PKData]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_Budget_PKMacroTipologia] FOREIGN KEY ([PKMacroTipologia]) REFERENCES [Dim].[MacroTipologia] ([PKMacroTipologia])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_Budget_PKData_PKCapoArea_PKMacroTipologia]
    ON [Fact].[Budget]([PKData] ASC, [PKCapoArea] ASC, [PKMacroTipologia] ASC);


GO

