CREATE TABLE [Staging].[CometaVendor] (
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
    [tipo]                   CHAR (1)       NULL,
    [RagioneSociale]         NVARCHAR (120) NULL,
    [indirizzo]              NVARCHAR (60)  NULL,
    [cap]                    NVARCHAR (10)  NULL,
    [localita]               NVARCHAR (60)  NULL,
    [provincia]              NVARCHAR (10)  NULL,
    [nazione]                NVARCHAR (60)  NULL,
    [cod_fiscale]            NVARCHAR (60)  NULL,
    [par_iva]                NVARCHAR (60)  NULL,
    CONSTRAINT [PK_Staging_CometaVendor] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_sog_commerciale] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_CometaVendor_BusinessKey]
    ON [Staging].[CometaVendor]([id_sog_commerciale] ASC);


GO

