CREATE TABLE [Fact].[Accessi] (
    [PKAccessi]              INT            CONSTRAINT [DFT_Fact_Accessi_PKAccessi] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_Accessi]) NOT NULL,
    [PKData]                 DATE           NOT NULL,
    [PKCliente]              INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NOT NULL,
    [NumeroAccessi]          INT            NOT NULL,
    [NumeroPagineVisitate]   INT            NOT NULL,
    [PKCapoArea]             INT            NULL,
    CONSTRAINT [PK_Fact_Accessi] PRIMARY KEY CLUSTERED ([PKAccessi] ASC),
    CONSTRAINT [FK_Fact_Accessi_PKCapoArea] FOREIGN KEY ([PKCapoArea]) REFERENCES [Dim].[CapoArea] ([PKCapoArea]),
    CONSTRAINT [FK_Fact_Accessi_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_Accessi_PKData] FOREIGN KEY ([PKData]) REFERENCES [Dim].[Data] ([PKData])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_Accessi_PKData_PKCliente]
    ON [Fact].[Accessi]([PKData] ASC, [PKCliente] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_Fact_Accessi_PKData_INCLUDE]
    ON [Fact].[Accessi]([PKData] ASC)
    INCLUDE([PKCliente], [NumeroAccessi], [NumeroPagineVisitate]);


GO

