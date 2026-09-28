CREATE TABLE [Staging].[CapoArea] (
    [CapoArea]               NVARCHAR (60)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    CONSTRAINT [PK_Staging_CapoArea] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [CapoArea] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_CapoArea_BusinessKey]
    ON [Staging].[CapoArea]([CapoArea] ASC);


GO

