CREATE TABLE [Fact].[Crediti] (
    [PKCrediti]              INT            CONSTRAINT [DFT_Fact_Crediti_PKCrediti] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_Crediti]) NOT NULL,
    [ID]                     INT            NOT NULL,
    [PKCorso]                INT            NOT NULL,
    [PKDataCreazione]        DATE           NOT NULL,
    [AnnoCreazione]          AS             (datepart(year,[PKDataCreazione])),
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NOT NULL,
    [IDCorso]                NVARCHAR (100) NOT NULL,
    [Nome]                   NVARCHAR (100) NOT NULL,
    [Cognome]                NVARCHAR (100) NOT NULL,
    [CodiceFiscale]          NVARCHAR (20)  NOT NULL,
    [EMail]                  NVARCHAR (100) NOT NULL,
    [Professione]            NVARCHAR (100) NOT NULL,
    [Ordine]                 NVARCHAR (100) NOT NULL,
    [EnteAccreditante]       NVARCHAR (100) NOT NULL,
    [TipoCrediti]            NVARCHAR (100) NOT NULL,
    [StatoCrediti]           NVARCHAR (40)  NOT NULL,
    [CodiceMateria]          NVARCHAR (50)  NOT NULL,
    [Crediti]                INT            NOT NULL,
    CONSTRAINT [PK_Fact_Crediti] PRIMARY KEY CLUSTERED ([PKCrediti] ASC),
    CONSTRAINT [FK_Fact_Crediti_PKCorso] FOREIGN KEY ([PKCorso]) REFERENCES [Dim].[Corso] ([PKCorso]),
    CONSTRAINT [FK_Fact_Crediti_PKDataCreazione] FOREIGN KEY ([PKDataCreazione]) REFERENCES [Dim].[Data] ([PKData])
);


GO

CREATE NONCLUSTERED INDEX [IX_Fact_Crediti_AnnoCreazione_CodiceFiscale]
    ON [Fact].[Crediti]([AnnoCreazione] ASC, [CodiceFiscale] ASC);


GO

