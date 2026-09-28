CREATE TABLE [Landing].[COMETA_MySolutionUsers] (
    [EMail]                  NVARCHAR (60)  NOT NULL,
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
    [num_progressivo]        NUMERIC (18)   NULL,
    [num_documento]          NVARCHAR (MAX) NULL,
    [data_documento]         DATETIME2 (7)  NULL,
    [data_inizio_contratto]  DATETIME2 (7)  NULL,
    [data_fine_contratto]    DATETIME2 (7)  NULL,
    [HaSconto]               BIT            NULL,
    [Nome]                   NVARCHAR (MAX) NULL,
    [Cognome]                NVARCHAR (MAX) NULL,
    [Quote]                  NUMERIC (18)   NOT NULL,
    [telefono_descrizione]   NVARCHAR (MAX) NOT NULL,
    [id_telefono]            INT            NOT NULL,
    [id_sog_commerciale]     INT            NOT NULL,
    [tipo]                   VARCHAR (20)   NOT NULL,
    [id_documento]           INT            NULL,
    CONSTRAINT [PK_Landing_COMETA_MySolutionUsers] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [EMail] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_MySolutionUsers_BusinessKey]
    ON [Landing].[COMETA_MySolutionUsers]([EMail] ASC);


GO

