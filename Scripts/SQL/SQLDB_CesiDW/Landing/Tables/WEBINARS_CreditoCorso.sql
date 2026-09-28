CREATE TABLE [Landing].[WEBINARS_CreditoCorso] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [CreditoTipologiaID]     INT            NOT NULL,
    [WebinarSource]          VARCHAR (50)   NOT NULL,
    [Autocertificazione]     BIT            NOT NULL,
    [Crediti]                TINYINT        NOT NULL,
    [Ora]                    INT            NULL,
    [CodiceMateria]          VARCHAR (50)   NULL,
    CONSTRAINT [PK_Landing_WEBINARS_CreditoCorso] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_CreditoCorso_BusinessKey]
    ON [Landing].[WEBINARS_CreditoCorso]([Id] ASC);


GO

