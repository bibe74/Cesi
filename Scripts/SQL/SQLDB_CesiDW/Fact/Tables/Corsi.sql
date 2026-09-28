CREATE TABLE [Fact].[Corsi] (
    [PKCorsi]                   INT             CONSTRAINT [DFT_Fact_Corsi_PKCorsi] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_Corsi]) NOT NULL,
    [OrderItemId]               INT             NOT NULL,
    [Partecipant_Id]            INT             NOT NULL,
    [PKUtente]                  INT             NOT NULL,
    [PKDataInizio]              DATE            NOT NULL,
    [PKDataIscrizione]          DATE            NOT NULL,
    [HistoricalHashKey]         VARBINARY (20)  NULL,
    [ChangeHashKey]             VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)    NULL,
    [InsertDatetime]            DATETIME        NOT NULL,
    [UpdateDatetime]            DATETIME        NOT NULL,
    [IsDeleted]                 BIT             NOT NULL,
    [NomePartecipante]          NVARCHAR (120)  NULL,
    [CognomePartecipante]       NVARCHAR (120)  NULL,
    [EmailPartecipante]         NVARCHAR (120)  NULL,
    [CodiceFiscalePartecipante] NVARCHAR (20)   NULL,
    [EmailPartecipanteRoot]     NVARCHAR (120)  NULL,
    [Utente]                    NVARCHAR (120)  NULL,
    [Corso]                     NVARCHAR (240)  NOT NULL,
    [IDCorso]                   NVARCHAR (20)   NULL,
    [IDWebinar]                 NVARCHAR (120)  NULL,
    [TipoCorso]                 NVARCHAR (120)  NULL,
    [HasDateMultiple]           BIT             NULL,
    [DescrizioneOrdine]         NVARCHAR (240)  NULL,
    [PrezzoUnitarioOrdine]      DECIMAL (10, 2) NOT NULL,
    [NumeroOrdine]              NVARCHAR (120)  NOT NULL,
    [StatoOrdine]               INT             NOT NULL,
    [ImportoTotaleOrdine]       DECIMAL (10, 2) NULL,
    CONSTRAINT [PK_Fact_Corsi] PRIMARY KEY CLUSTERED ([PKCorsi] ASC),
    CONSTRAINT [FK_Fact_Corsi_PKDataInizio] FOREIGN KEY ([PKDataInizio]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_Corsi_PKDataIscrizione] FOREIGN KEY ([PKDataIscrizione]) REFERENCES [Dim].[Data] ([PKData]),
    CONSTRAINT [FK_Fact_Corsi_PKUtente] FOREIGN KEY ([PKUtente]) REFERENCES [Dim].[Utente] ([PKUtente])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Fact_Corsi_OrderItemId_Partecipant_Id]
    ON [Fact].[Corsi]([OrderItemId] ASC, [Partecipant_Id] ASC);


GO

