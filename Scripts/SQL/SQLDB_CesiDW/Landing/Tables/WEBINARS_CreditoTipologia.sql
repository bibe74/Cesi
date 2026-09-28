CREATE TABLE [Landing].[WEBINARS_CreditoTipologia] (
    [ID]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Ordine]                 VARCHAR (100)  NOT NULL,
    [Tipo]                   VARCHAR (100)  NOT NULL,
    CONSTRAINT [PK_Landing_WEBINARS_CreditoTipologia] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_CreditoTipologia_BusinessKey]
    ON [Landing].[WEBINARS_CreditoTipologia]([ID] ASC);


GO

