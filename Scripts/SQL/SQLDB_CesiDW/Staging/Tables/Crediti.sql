CREATE TABLE [Staging].[Crediti] (
    [ID]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [IDCorso]                VARCHAR (100)  NOT NULL,
    [PKCorso]                INT            NOT NULL,
    [Nome]                   VARCHAR (100)  NULL,
    [Cognome]                VARCHAR (100)  NULL,
    [CodiceFiscale]          VARCHAR (20)   NULL,
    [Professione]            NVARCHAR (100) NULL,
    [Ordine]                 NVARCHAR (100) NULL,
    [PKDataCreazione]        DATE           NOT NULL,
    [Email]                  VARCHAR (100)  NULL,
    [EnteAccreditante]       VARCHAR (100)  NOT NULL,
    [TipoCrediti]            NVARCHAR (40)  NULL,
    [StatoCrediti]           NVARCHAR (40)  NULL,
    [CodiceMateria]          NVARCHAR (50)  NOT NULL,
    [Crediti]                TINYINT        NOT NULL,
    CONSTRAINT [PK_Landing_WEBINARS_CreditoAutocertificazione] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_CreditoAutocertificazione_BusinessKey]
    ON [Staging].[Crediti]([ID] ASC);


GO

