CREATE TABLE [Import].[Budget] (
    [PKDataInizioMese]          DATE            NOT NULL,
    [CapoArea]                  NVARCHAR (60)   NOT NULL,
    [ImportoBudgetNuoveVendite] DECIMAL (18, 2) NOT NULL,
    [ImportoBudgetRinnovi]      DECIMAL (18, 2) NOT NULL,
    CONSTRAINT [PK_Import_Budget] PRIMARY KEY CLUSTERED ([PKDataInizioMese] ASC, [CapoArea] ASC)
);


GO

