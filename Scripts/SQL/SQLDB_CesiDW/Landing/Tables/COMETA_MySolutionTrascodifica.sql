CREATE TABLE [Landing].[COMETA_MySolutionTrascodifica] (
    [codice]                 NVARCHAR (40)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [tipo]                   NVARCHAR (20)  NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_MySolutionTrascodifica] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [codice] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_MySolutionTrascodifica_BusinessKey]
    ON [Landing].[COMETA_MySolutionTrascodifica]([codice] ASC);


GO

