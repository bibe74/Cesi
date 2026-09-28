CREATE TABLE [Dim].[Cliente] (
    [PKCliente]                            INT            CONSTRAINT [DFT_Dim_Cliente_PKCliente] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_Cliente]) NOT NULL,
    [IDSoggettoCommerciale]                INT            NOT NULL,
    [HistoricalHashKey]                    VARBINARY (20) NULL,
    [ChangeHashKey]                        VARBINARY (20) NULL,
    [HistoricalHashKeyASCII]               VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]                   VARCHAR (34)   NULL,
    [InsertDatetime]                       DATETIME       CONSTRAINT [DFT_Dim_Cliente_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]                       DATETIME       CONSTRAINT [DFT_Dim_Cliente_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]                            BIT            CONSTRAINT [DFT_Dim_Cliente_IsDeleted] DEFAULT ((0)) NOT NULL,
    [Email]                                NVARCHAR (120) NOT NULL,
    [IDAnagraficaCometa]                   INT            CONSTRAINT [DFT_Dim_Cliente_IDAnagraficaCometa] DEFAULT ((-1)) NULL,
    [HasAnagraficaCometa]                  BIT            CONSTRAINT [DFT_Dim_Cliente_HasAnagraficaCometa] DEFAULT ((0)) NOT NULL,
    [HasAnagraficaNopCommerce]             BIT            CONSTRAINT [DFT_Dim_Cliente_HasAnagraficaNopCommerce] DEFAULT ((0)) NOT NULL,
    [HasAnagraficaMySolution]              BIT            CONSTRAINT [DFT_Dim_Cliente_HasAnagraficaMySolution] DEFAULT ((0)) NOT NULL,
    [ProvenienzaAnagrafica]                NVARCHAR (20)  CONSTRAINT [DFT_Dim_Cliente_ProvenienzaAnagrafica] DEFAULT (N'') NOT NULL,
    [CodiceCliente]                        NVARCHAR (10)  NOT NULL,
    [TipoSoggettoCommerciale]              NVARCHAR (10)  CONSTRAINT [DFT_Dim_Cliente_TipoSoggettoCommerciale] DEFAULT (N'') NOT NULL,
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
    [Telefono]                             NVARCHAR (60)  NOT NULL,
    [Cellulare]                            NVARCHAR (60)  NOT NULL,
    [Fax]                                  NVARCHAR (60)  NOT NULL,
    [TipoCliente]                          NVARCHAR (20)  NOT NULL,
    [Agente]                               NVARCHAR (60)  NOT NULL,
    [PKDataInizioContratto]                DATE           CONSTRAINT [DFT_Dim_Cliente_PKDataInizioContratto] DEFAULT (CONVERT([date],'19000101')) NOT NULL,
    [PKDataFineContratto]                  DATE           CONSTRAINT [DFT_Dim_Cliente_PKDataFineContratto] DEFAULT (CONVERT([date],'19000101')) NOT NULL,
    [PKDataDisdetta]                       DATE           CONSTRAINT [DFT_Dim_Cliente_PKDataDisdetta] DEFAULT (CONVERT([date],'19000101')) NOT NULL,
    [MotivoDisdetta]                       NVARCHAR (120) NOT NULL,
    [PKGruppoAgenti]                       INT            CONSTRAINT [DFT_Dim_Cliente_PKGruppoAgenti] DEFAULT ((-1)) NOT NULL,
    [Cognome]                              NVARCHAR (60)  NOT NULL,
    [Nome]                                 NVARCHAR (60)  NOT NULL,
    [IsAttivo]                             BIT            CONSTRAINT [DFT_Dim_Cliente_IsAttivo] DEFAULT ((0)) NOT NULL,
    [IsAbbonato]                           BIT            CONSTRAINT [DFT_Dim_Cliente_IsAbbonato] DEFAULT ((0)) NOT NULL,
    [IDSoggettoCommerciale_migrazione]     INT            NULL,
    [IDSoggettoCommerciale_migrazione_old] INT            NULL,
    [IDProvincia]                          NVARCHAR (10)  CONSTRAINT [DFT_Dim_Cliente_IDProvincia] DEFAULT (N'') NOT NULL,
    [IsClienteFormazione]                  BIT            CONSTRAINT [DFT_Dim_Cliente_IsClienteFormazione] DEFAULT ((0)) NOT NULL,
    [CapoAreaDefault]                      NVARCHAR (60)  CONSTRAINT [DFT_Dim_Cliente_CapoAreaDefault] DEFAULT (N'') NOT NULL,
    [AgenteDefault]                        NVARCHAR (60)  CONSTRAINT [DFT_Dim_Cliente_AgenteDefault] DEFAULT (N'') NOT NULL,
    [HasRoleMySolutionDemo]                BIT            CONSTRAINT [DFT_Dim_Cliente_HasRoleMySolutionDemo] DEFAULT ((0)) NOT NULL,
    [HasRoleMySolutionInterno]             BIT            CONSTRAINT [DFT_Dim_Cliente_HasRoleMySolutionInterno] DEFAULT ((0)) NOT NULL,
    [HasAbbonamentoMySolution]             BIT            CONSTRAINT [DFT_Dim_Cliente_HasAbbonamentoMySolution] DEFAULT ((0)) NOT NULL,
    [HasAbbonamentoMIA]                    BIT            CONSTRAINT [DFT_Dim_Cliente_HasAbbonamentoMIA] DEFAULT ((0)) NOT NULL,
    [AgenteZoho]                           NVARCHAR (60)  CONSTRAINT [DFT_Dim_Cliente_AgenteZoho] DEFAULT (N'') NOT NULL,
    [IDProfessione]                        INT            CONSTRAINT [DFT_Dim_Cliente_IDProfessione] DEFAULT ((-1)) NOT NULL,
    [Professione]                          NVARCHAR (60)  CONSTRAINT [DFT_Dim_Cliente_Professione] DEFAULT (N'') NOT NULL,
    CONSTRAINT [PK_Dim_Cliente] PRIMARY KEY CLUSTERED ([PKCliente] ASC),
    CONSTRAINT [FK_Dim_Cliente_PKDataDisdetta] FOREIGN KEY ([PKDataDisdetta]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Dim_Cliente_PKDataFineContratto] FOREIGN KEY ([PKDataFineContratto]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Dim_Cliente_PKDataInizioContratto] FOREIGN KEY ([PKDataInizioContratto]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Dim_Cliente_PKGruppoAgenti] FOREIGN KEY ([PKGruppoAgenti]) REFERENCES [Dim].[GruppoAgenti] ([PKGruppoAgenti])
);


GO

CREATE NONCLUSTERED INDEX [IX_Dim_Cliente_PKGruppoAgenti_IsAbbonato_PKDataFineContratto_INCLUDE]
    ON [Dim].[Cliente]([PKGruppoAgenti] ASC, [IsAbbonato] ASC, [PKDataFineContratto] ASC)
    INCLUDE([Email], [CodiceCliente], [RagioneSociale], [Localita], [Regione], [Telefono], [TipoCliente], [PKDataInizioContratto], [IDProvincia]);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Cliente_IDSoggettoCommerciale]
    ON [Dim].[Cliente]([IDSoggettoCommerciale] ASC);


GO

