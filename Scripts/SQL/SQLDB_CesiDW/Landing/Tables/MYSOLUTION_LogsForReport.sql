CREATE TABLE [Landing].[MYSOLUTION_LogsForReport] (
    [Data]                   DATE           NOT NULL,
    [Username]               NVARCHAR (50)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [NumeroAccessi]          INT            NULL,
    [NumeroPagineVisitate]   INT            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_LogsForReport] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Data] ASC, [Username] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_LogsForReport_BusinessKey]
    ON [Landing].[MYSOLUTION_LogsForReport]([Data] ASC, [Username] ASC);


GO

