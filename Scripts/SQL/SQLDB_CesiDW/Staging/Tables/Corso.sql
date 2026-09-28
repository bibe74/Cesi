CREATE TABLE [Staging].[Corso] (
    [IDCorso]                NVARCHAR (50)   NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [Corso]                  NVARCHAR (500)  NULL,
    [TipoCorso]              VARCHAR (500)   NULL,
    [Giornata]               NVARCHAR (500)  NULL,
    [PKDataInizioCorso]      DATE            NOT NULL,
    [OraInizioCorso]         NVARCHAR (4000) NULL,
    CONSTRAINT [PK_Landing_WEBINARS_WeBinars] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDCorso] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_WeBinars_BusinessKey]
    ON [Staging].[Corso]([IDCorso] ASC);


GO

