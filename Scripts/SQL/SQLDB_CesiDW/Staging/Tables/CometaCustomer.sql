CREATE TABLE [Staging].[CometaCustomer] (
    [id_sog_commerciale]     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (10)  NULL,
    [id_anagrafica]          INT            NULL,
    [tipo]                   VARCHAR (1)    NULL,
    [id_gruppo_agenti]       INT            NULL,
    [RagioneSociale]         NVARCHAR (120) NULL,
    [indirizzo]              NVARCHAR (60)  NULL,
    [cap]                    NVARCHAR (10)  NULL,
    [localita]               NVARCHAR (60)  NULL,
    [provincia]              NVARCHAR (10)  NULL,
    [nazione]                NVARCHAR (60)  NULL,
    [cod_fiscale]            NVARCHAR (60)  NULL,
    [par_iva]                NVARCHAR (60)  NULL,
    [Email]                  NVARCHAR (200) NULL,
    [nome]                   NVARCHAR (MAX) NULL,
    [cognome]                NVARCHAR (MAX) NULL,
    [Quote]                  NUMERIC (18)   NULL,
    [telefono_descrizione]   NVARCHAR (400) NULL,
    [id_telefono]            INT            NULL,
    [id_documento]           INT            NULL,
    [num_documento]          NVARCHAR (20)  NULL,
    [data_documento]         DATE           NULL,
    [data_inizio_contratto]  DATE           NULL,
    [data_fine_contratto]    DATE           NULL,
    [HasSconto]              INT            NULL,
    [data_disdetta]          DATE           NULL,
    [motivo_disdetta]        NVARCHAR (120) NULL,
    [Telefono]               NVARCHAR (200) NULL,
    [Cellulare]              NVARCHAR (200) NULL,
    [Fax]                    NVARCHAR (200) NULL,
    [IDProfessione]          INT            NULL,
    [Professione]            NVARCHAR (60)  NULL,
    CONSTRAINT [PK_Staging_CometaCustomer] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_sog_commerciale] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_CometaCustomer_BusinessKey]
    ON [Staging].[CometaCustomer]([id_sog_commerciale] ASC);


GO

