CREATE TABLE [Fact].[Analytics] (
    [PKAnalytics]       INT            CONSTRAINT [DFT_PKAnalytics] DEFAULT (NEXT VALUE FOR [seq_Fact_Analytics]) NOT NULL,
    [PKDataVisita]      DATE           NOT NULL,
    [PKCliente]         INT            NOT NULL,
    [Percorso]          NVARCHAR (255) NOT NULL,
    [HistoricalHashKey] VARBINARY (32) NULL,
    [ChangeHashKey]     VARBINARY (32) NULL,
    [InsertDatetime]    DATETIME       NOT NULL,
    [UpdateDatetime]    DATETIME       NOT NULL,
    [IsDeleted]         BIT            NULL,
    [NumeroVisite]      INT            NULL,
    CONSTRAINT [PK_Fact_Analytics] PRIMARY KEY CLUSTERED ([PKAnalytics] ASC),
    CONSTRAINT [FK_Fact_Analytics_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_Analytics_PKDataVisita] FOREIGN KEY ([PKDataVisita]) REFERENCES [Dim].[Data] ([PKData])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_Analytics_BusinessKey]
    ON [Fact].[Analytics]([PKDataVisita] ASC, [PKCliente] ASC, [Percorso] ASC);


GO

