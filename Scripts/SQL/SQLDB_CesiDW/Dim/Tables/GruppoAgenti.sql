CREATE TABLE [Dim].[GruppoAgenti] (
    [PKGruppoAgenti]         INT            CONSTRAINT [DFT_Dim_GruppoAgenti_PKGruppoAgenti] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_GruppoAgenti]) NOT NULL,
    [id_gruppo_agenti]       INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       CONSTRAINT [DFT_Dim_GruppoAgenti_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]         DATETIME       CONSTRAINT [DFT_Dim_GruppoAgenti_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]              BIT            CONSTRAINT [DFT_Dim_GruppoAgenti_IsDeleted] DEFAULT ((0)) NOT NULL,
    [IDGruppoAgenti]         NVARCHAR (10)  CONSTRAINT [DFT_Dim_GruppoAgenti_IDGruppoAgenti] DEFAULT (N'') NOT NULL,
    [GruppoAgenti]           NVARCHAR (60)  CONSTRAINT [DFT_Dim_GruppoAgenti_GruppoAgenti] DEFAULT (N'') NOT NULL,
    [CapoArea]               NVARCHAR (60)  CONSTRAINT [DFT_Dim_GruppoAgenti_CapoArea] DEFAULT (N'') NOT NULL,
    [Agente]                 NVARCHAR (60)  CONSTRAINT [DFT_Dim_GruppoAgenti_Agente] DEFAULT (N'') NOT NULL,
    [Subagente]              NVARCHAR (60)  CONSTRAINT [DFT_Dim_GruppoAgenti_Subagente] DEFAULT (N'') NOT NULL,
    [PKCapoArea]             INT            CONSTRAINT [DFT_Dim_GruppoAgenti_PKCapoArea] DEFAULT ((-1)) NOT NULL,
    CONSTRAINT [PK_Dim_GruppoAgenti] PRIMARY KEY CLUSTERED ([PKGruppoAgenti] ASC),
    CONSTRAINT [FK_Dim_GruppoAgenti_PKCapoArea] FOREIGN KEY ([PKCapoArea]) REFERENCES [Dim].[CapoArea] ([PKCapoArea])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_GruppoAgenti_GUIDGruppoAgenti]
    ON [Dim].[GruppoAgenti]([id_gruppo_agenti] ASC);


GO

