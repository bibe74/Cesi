CREATE TABLE [Landing].[COMETA_Esercizio] (
    [id_esercizio]           INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 CHAR (4)       NULL,
    [data_inizio]            DATE           NULL,
    [data_fine]              DATE           NULL,
    CONSTRAINT [PK_Landing_COMETA_Esercizio] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_esercizio] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Esercizio_BusinessKey]
    ON [Landing].[COMETA_Esercizio]([id_esercizio] ASC);


GO

