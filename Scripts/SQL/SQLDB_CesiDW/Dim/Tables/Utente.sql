CREATE TABLE [Dim].[Utente] (
    [PKUtente]               INT            CONSTRAINT [DFT_Dim_Utente_PKUtente] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_Utente]) NOT NULL,
    [IDUtente]               INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       CONSTRAINT [DFT_Dim_Utente_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]         DATETIME       CONSTRAINT [DFT_Dim_Utente_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]              BIT            CONSTRAINT [DFT_Dim_Utente_IsDeleted] DEFAULT ((0)) NOT NULL,
    [Email]                  NVARCHAR (60)  NOT NULL,
    [RagioneSociale]         NVARCHAR (120) NOT NULL,
    [Nome]                   NVARCHAR (60)  NOT NULL,
    [Cognome]                NVARCHAR (60)  NOT NULL,
    [Citta]                  NVARCHAR (60)  NOT NULL,
    [PKCliente]              INT            NOT NULL,
    CONSTRAINT [PK_Dim_Utente] PRIMARY KEY CLUSTERED ([PKUtente] ASC),
    CONSTRAINT [FK_Dim_Utente_PKCliente] FOREIGN KEY ([PKCliente]) REFERENCES [Dim].[Cliente] ([PKCliente])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Utente_id_Utente]
    ON [Dim].[Utente]([IDUtente] ASC);


GO

