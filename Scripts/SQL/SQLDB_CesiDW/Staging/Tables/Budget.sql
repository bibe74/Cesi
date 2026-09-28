CREATE TABLE [Staging].[Budget] (
    [PKData]                    DATE            NOT NULL,
    [PKCapoArea]                INT             NOT NULL,
    [PKMacroTipologia]          INT             NOT NULL,
    [HistoricalHashKey]         VARBINARY (20)  NULL,
    [ChangeHashKey]             VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)    NULL,
    [InsertDatetime]            DATETIME        NOT NULL,
    [UpdateDatetime]            DATETIME        NOT NULL,
    [IsDeleted]                 BIT             NULL,
    [ImportoBudgetNuoveVendite] DECIMAL (18, 2) NULL,
    [ImportoBudgetRinnovi]      DECIMAL (18, 2) NULL,
    [ImportoBudget]             DECIMAL (18, 2) NOT NULL,
    CONSTRAINT [PK_Staging_Budget] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [PKData] ASC, [PKCapoArea] ASC, [PKMacroTipologia] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_Budget_BusinessKey]
    ON [Staging].[Budget]([PKData] ASC, [PKCapoArea] ASC, [PKMacroTipologia] ASC);


GO

