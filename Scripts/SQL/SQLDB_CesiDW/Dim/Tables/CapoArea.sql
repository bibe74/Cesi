CREATE TABLE [Dim].[CapoArea] (
    [PKCapoArea]             INT            CONSTRAINT [DFT_Dim_CapoArea_PKCapoArea] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_CapoArea]) NOT NULL,
    [CapoArea]               NVARCHAR (60)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       CONSTRAINT [DFT_Dim_CapoArea_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]         DATETIME       CONSTRAINT [DFT_Dim_CapoArea_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]              BIT            CONSTRAINT [DFT_Dim_CapoArea_IsDeleted] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Dim_CapoArea] PRIMARY KEY CLUSTERED ([PKCapoArea] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_CapoArea_GUIDCapoArea]
    ON [Dim].[CapoArea]([CapoArea] ASC);


GO

