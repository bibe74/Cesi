CREATE TABLE [Landing].[WEBINARS_CreditoAutocertificazione] (
    [ID]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [AutocertificazioneID]   INT            NOT NULL,
    [CreditoTipologiaID]     INT            NOT NULL,
    [CreditoCorsoID]         INT            NULL,
    [Stato]                  TINYINT        NOT NULL,
    [Crediti]                TINYINT        NOT NULL,
    CONSTRAINT [PK_Landing_WEBINARS_CreditoAutocertificazione] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_CreditoAutocertificazione_BusinessKey]
    ON [Landing].[WEBINARS_CreditoAutocertificazione]([ID] ASC);


GO

