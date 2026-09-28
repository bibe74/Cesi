CREATE TABLE [Staging].[GruppoAgenti] (
    [id_gruppo_agenti]       INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [IDGruppoAgenti]         NVARCHAR (10)  NOT NULL,
    [GruppoAgenti]           NVARCHAR (60)  NOT NULL,
    [CapoArea]               NVARCHAR (60)  NOT NULL,
    [Agente]                 NVARCHAR (60)  NOT NULL,
    [Subagente]              NVARCHAR (60)  NOT NULL,
    CONSTRAINT [PK_Staging_Gruppo_Agenti] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_gruppo_agenti] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_GruppoAgenti_BusinessKey]
    ON [Staging].[GruppoAgenti]([id_gruppo_agenti] ASC);


GO

