CREATE TABLE [Landing].[COMETA_MySolutionContracts] (
    [id_riga_documento]      INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [id_anagrafica]          INT            NULL,
    [codice]                 NVARCHAR (MAX) NULL,
    [RagioneSociale]         NVARCHAR (MAX) NOT NULL,
    [indirizzo]              NVARCHAR (MAX) NULL,
    [cap]                    NVARCHAR (MAX) NULL,
    [localita]               NVARCHAR (MAX) NULL,
    [provincia]              NVARCHAR (MAX) NULL,
    [nazione]                NVARCHAR (MAX) NULL,
    [cod_fiscale]            NVARCHAR (MAX) NULL,
    [par_iva]                NVARCHAR (MAX) NULL,
    [EMail]                  NVARCHAR (MAX) NOT NULL,
    [num_progressivo]        NUMERIC (18)   NULL,
    [num_documento]          NVARCHAR (MAX) NULL,
    [data_documento]         DATETIME2 (7)  NULL,
    [data_inizio_contratto]  DATETIME2 (7)  NULL,
    [data_fine_contratto]    DATETIME2 (7)  NULL,
    [Nome]                   NVARCHAR (MAX) NULL,
    [Cognome]                NVARCHAR (MAX) NULL,
    [Quote]                  NUMERIC (18)   NOT NULL,
    [id_sog_commerciale]     INT            NOT NULL,
    [tipo]                   NVARCHAR (MAX) NULL,
    [id_documento]           INT            NULL,
    [descrizione]            NVARCHAR (MAX) NULL,
    [id_articolo]            INT            NULL,
    [prezzo]                 NUMERIC (18)   NULL,
    [sconto]                 NVARCHAR (MAX) NULL,
    [prezzo_netto]           NUMERIC (18)   NULL,
    [prezzo_netto_ivato]     NUMERIC (18)   NULL,
    [note_intestazione]      NVARCHAR (MAX) NULL,
    [data_disdetta]          DATETIME2 (7)  NULL,
    [motivo_disdetta]        NVARCHAR (MAX) NULL,
    [pec]                    NVARCHAR (MAX) NULL,
    [CodiceArticolo]         NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_Landing_COMETA_MySolutionContracts] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_riga_documento] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_MySolutionContracts_BusinessKey]
    ON [Landing].[COMETA_MySolutionContracts]([id_riga_documento] ASC);


GO

