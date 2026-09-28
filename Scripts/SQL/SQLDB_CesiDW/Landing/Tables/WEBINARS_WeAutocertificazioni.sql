CREATE TABLE [Landing].[WEBINARS_WeAutocertificazioni] (
    [ID]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Corso]                  VARCHAR (100)  NOT NULL,
    [Nome]                   VARCHAR (100)  NOT NULL,
    [Cognome]                VARCHAR (100)  NOT NULL,
    [CodiceFiscale]          VARCHAR (20)   NOT NULL,
    [Professione]            VARCHAR (100)  NULL,
    [Ordine]                 VARCHAR (100)  NULL,
    [DataCreazione]          DATE           NULL,
    [Email]                  VARCHAR (100)  NOT NULL,
    CONSTRAINT [PK_Landing_WEBINARS_WeAutocertificazioni] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_WeAutocertificazioni_BusinessKey]
    ON [Landing].[WEBINARS_WeAutocertificazioni]([ID] ASC);


GO

