CREATE TABLE [Staging].[Cliente] (
    [IDSoggettoCommerciale]                INT            NOT NULL,
    [HistoricalHashKey]                    VARBINARY (20) NULL,
    [ChangeHashKey]                        VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]               VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]                   VARCHAR (34)   NULL,
    [InsertDatetime]                       DATETIME       NOT NULL,
    [UpdateDatetime]                       DATETIME       NOT NULL,
    [IsDeleted]                            BIT            NULL,
    [Email]                                NVARCHAR (80)  NOT NULL,
    [IDAnagraficaCometa]                   INT            NOT NULL,
    [HasAnagraficaCometa]                  BIT            NOT NULL,
    [HasAnagraficaNopCommerce]             BIT            NOT NULL,
    [HasAnagraficaMySolution]              BIT            NOT NULL,
    [ProvenienzaAnagrafica]                NVARCHAR (20)  NOT NULL,
    [CodiceCliente]                        NVARCHAR (10)  NOT NULL,
    [TipoSoggettoCommerciale]              CHAR (1)       NOT NULL,
    [RagioneSociale]                       NVARCHAR (120) NOT NULL,
    [CodiceFiscale]                        NVARCHAR (20)  NOT NULL,
    [PartitaIVA]                           NVARCHAR (20)  NOT NULL,
    [Indirizzo]                            NVARCHAR (120) NOT NULL,
    [CAP]                                  NVARCHAR (10)  NOT NULL,
    [Localita]                             NVARCHAR (60)  NOT NULL,
    [Provincia]                            NVARCHAR (50)  NOT NULL,
    [Regione]                              NVARCHAR (60)  NOT NULL,
    [Macroregione]                         NVARCHAR (60)  NOT NULL,
    [Nazione]                              NVARCHAR (60)  NOT NULL,
    [TipoCliente]                          NVARCHAR (10)  NOT NULL,
    [Agente]                               NVARCHAR (60)  NOT NULL,
    [PKDataInizioContratto]                DATE           NOT NULL,
    [PKDataFineContratto]                  DATE           NOT NULL,
    [PKDataDisdetta]                       DATE           NOT NULL,
    [MotivoDisdetta]                       NVARCHAR (120) NOT NULL,
    [PKGruppoAgenti]                       INT            NOT NULL,
    [Cognome]                              NVARCHAR (60)  NOT NULL,
    [Nome]                                 NVARCHAR (60)  NOT NULL,
    [Telefono]                             NVARCHAR (60)  NOT NULL,
    [Cellulare]                            NVARCHAR (60)  NOT NULL,
    [Fax]                                  NVARCHAR (60)  NOT NULL,
    [IsAbbonato]                           BIT            NOT NULL,
    [IDSoggettoCommerciale_migrazione]     INT            NULL,
    [IDSoggettoCommerciale_migrazione_old] INT            NULL,
    [IDProvincia]                          NVARCHAR (10)  NOT NULL,
    [CapoAreaDefault]                      NVARCHAR (60)  NOT NULL,
    [AgenteDefault]                        NVARCHAR (60)  NOT NULL,
    [HasRoleMySolutionDemo]                BIT            NOT NULL,
    CONSTRAINT [PK_Staging_Cliente] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [IDSoggettoCommerciale] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_Cliente_BusinessKey]
    ON [Staging].[Cliente]([IDSoggettoCommerciale] ASC);


GO

