CREATE TABLE [Staging].[MySolutionUsers] (
    [Email]                           NVARCHAR (120) NOT NULL,
    [rnDataInizioContrattoDESC]       INT            NOT NULL,
    [HistoricalHashKey]               VARBINARY (20) NULL,
    [ChangeHashKey]                   VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]          VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]              VARCHAR (34)   NULL,
    [InsertDatetime]                  DATETIME       NOT NULL,
    [UpdateDatetime]                  DATETIME       NOT NULL,
    [IsDeleted]                       BIT            NULL,
    [rnCodiceDataInizioContrattoDESC] INT            NOT NULL,
    [CodiceCliente]                   NVARCHAR (10)  NOT NULL,
    [RagioneSociale]                  NVARCHAR (120) NOT NULL,
    [CodiceFiscale]                   NVARCHAR (20)  NOT NULL,
    [PartitaIVA]                      NVARCHAR (20)  NOT NULL,
    [Localita]                        NVARCHAR (60)  NOT NULL,
    [Provincia]                       NVARCHAR (10)  NOT NULL,
    [Telefono]                        NVARCHAR (60)  NOT NULL,
    [TipoCliente]                     NVARCHAR (10)  NOT NULL,
    [PKDataInizioContratto]           DATE           NOT NULL,
    [PKDataFineContratto]             DATE           NOT NULL,
    [Cognome]                         NVARCHAR (60)  NOT NULL,
    [Nome]                            NVARCHAR (60)  NOT NULL,
    [IDDocumento]                     INT            NOT NULL,
    CONSTRAINT [PK_Landing_COMETA_MySolutionUsers] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Email] ASC, [rnDataInizioContrattoDESC] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_MySolutionUsers_BusinessKey]
    ON [Staging].[MySolutionUsers]([Email] ASC, [rnDataInizioContrattoDESC] ASC);


GO

