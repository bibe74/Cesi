CREATE TABLE [Staging].[SoggettoCommerciale] (
    [IDSoggettoCommerciale]     INT            NOT NULL,
    [HistoricalHashKey]         VARBINARY (20) NULL,
    [ChangeHashKey]             VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)   NULL,
    [InsertDatetime]            DATETIME       NOT NULL,
    [UpdateDatetime]            DATETIME       NOT NULL,
    [IsDeleted]                 BIT            NULL,
    [CodiceSoggettoCommerciale] NVARCHAR (10)  NOT NULL,
    [IDAnagrafica]              INT            NOT NULL,
    [TipoSoggettoCommerciale]   CHAR (1)       NOT NULL,
    [RagioneSociale]            NVARCHAR (120) NOT NULL,
    [Indirizzo]                 NVARCHAR (120) NOT NULL,
    [CAP]                       NVARCHAR (10)  NOT NULL,
    [Localita]                  NVARCHAR (60)  NOT NULL,
    [Provincia]                 NVARCHAR (10)  NOT NULL,
    [Nazione]                   NVARCHAR (60)  NOT NULL,
    [CodiceFiscale]             NVARCHAR (20)  NOT NULL,
    [PartitaIVA]                NVARCHAR (20)  NOT NULL,
    [PKGruppoAgenti]            INT            NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_SoggettoCommerciale] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDSoggettoCommerciale] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_SoggettoCommerciale_BusinessKey]
    ON [Staging].[SoggettoCommerciale]([IDSoggettoCommerciale] ASC);


GO

