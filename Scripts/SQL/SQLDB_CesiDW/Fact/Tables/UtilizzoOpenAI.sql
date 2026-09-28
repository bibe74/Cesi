CREATE TABLE [Fact].[UtilizzoOpenAI] (
    [PKUtilizzoOpenAI]      INT             CONSTRAINT [DFT_PKUtilizzoOpenAI] DEFAULT (NEXT VALUE FOR [seq_Fact_UtilizzoOpenAI]) NOT NULL,
    [IDUtilizzoOpenAI]      INT             NOT NULL,
    [HistoricalHashKey]     VARBINARY (32)  NULL,
    [ChangeHashKey]         VARBINARY (32)  NULL,
    [InsertDatetime]        DATETIME        NOT NULL,
    [UpdateDatetime]        DATETIME        NOT NULL,
    [IsDeleted]             BIT             NULL,
    [PKCliente]             INT             NOT NULL,
    [IDThread]              NVARCHAR (200)  NOT NULL,
    [IDConversazione]       NVARCHAR (200)  NOT NULL,
    [PKDataConversazione]   DATE            NOT NULL,
    [Area]                  NVARCHAR (100)  NOT NULL,
    [ModalitaRisposta]      NVARCHAR (50)   NOT NULL,
    [Modello]               NVARCHAR (100)  NOT NULL,
    [IDVectorStorage]       NVARCHAR (200)  NULL,
    [InputTokens]           BIGINT          NULL,
    [OutputTokens]          BIGINT          NULL,
    [ReasoningTokens]       BIGINT          NULL,
    [TotalTokens]           BIGINT          NULL,
    [EstimatedTotalCostUSD] NUMERIC (18, 6) NULL,
    CONSTRAINT [PK_Fact_UtilizzoOpenAI] PRIMARY KEY CLUSTERED ([PKUtilizzoOpenAI] ASC),
    CONSTRAINT [FK_Fact_UtilizzoOpenAI_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente]),
    CONSTRAINT [FK_Fact_UtilizzoOpenAI_PKDataConversazione] FOREIGN KEY ([PKDataConversazione]) REFERENCES [Dim].[Data] ([PKData])
);


GO

