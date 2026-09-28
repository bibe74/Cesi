CREATE TABLE [Staging].[Corsi] (
    [OrderItemId]               INT             NOT NULL,
    [Partecipant_Id]            INT             NOT NULL,
    [HistoricalHashKey]         VARBINARY (20)  NULL,
    [ChangeHashKey]             VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)    NULL,
    [InsertDatetime]            DATETIME        NOT NULL,
    [UpdateDatetime]            DATETIME        NOT NULL,
    [IsDeleted]                 BIT             NULL,
    [NomePartecipante]          NVARCHAR (MAX)  NULL,
    [CognomePartecipante]       NVARCHAR (MAX)  NULL,
    [EmailPartecipante]         NVARCHAR (MAX)  NULL,
    [CodiceFiscalePartecipante] NVARCHAR (MAX)  NULL,
    [EmailPartecipanteRoot]     NVARCHAR (MAX)  NULL,
    [Utente]                    NVARCHAR (1000) NULL,
    [PKUtente]                  INT             NULL,
    [Corso]                     NVARCHAR (400)  NOT NULL,
    [IDCorso]                   NVARCHAR (400)  NULL,
    [IDWebinar]                 NVARCHAR (400)  NULL,
    [TipoCorso]                 NVARCHAR (MAX)  NULL,
    [PKDataInizio]              DATE            NULL,
    [HasDateMultiple]           BIT             NULL,
    [DescrizioneOrdine]         NVARCHAR (MAX)  NULL,
    [PrezzoUnitarioOrdine]      NUMERIC (18, 4) NOT NULL,
    [ImportoTotaleOrdine]       NUMERIC (18, 4) NOT NULL,
    [NumeroOrdine]              NVARCHAR (MAX)  NOT NULL,
    [StatoOrdine]               INT             NOT NULL,
    [PKDataIscrizione]          DATE            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_CoursesData] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [OrderItemId] ASC, [Partecipant_Id] ASC)
);


GO

