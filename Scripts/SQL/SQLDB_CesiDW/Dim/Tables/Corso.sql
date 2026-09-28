CREATE TABLE [Dim].[Corso] (
    [PKCorso]                INT            CONSTRAINT [DFT_Dim_Corso_PKCorso] DEFAULT (NEXT VALUE FOR [dbo].[seq_Dim_Corso]) NOT NULL,
    [IDCorso]                NVARCHAR (50)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       CONSTRAINT [DFT_Dim_Corso_InsertDatetime] DEFAULT (getdate()) NOT NULL,
    [UpdateDatetime]         DATETIME       CONSTRAINT [DFT_Dim_Corso_UpdateDatetime] DEFAULT (getdate()) NOT NULL,
    [IsDeleted]              BIT            CONSTRAINT [DFT_Dim_Corso_IsDeleted] DEFAULT ((0)) NOT NULL,
    [Corso]                  NVARCHAR (500) NULL,
    [TipoCorso]              NVARCHAR (500) NULL,
    [Giornata]               NVARCHAR (500) NULL,
    [PKDataInizioCorso]      DATE           NOT NULL,
    [OraInizioCorso]         NVARCHAR (10)  NULL,
    CONSTRAINT [PK_Dim_Corso] PRIMARY KEY CLUSTERED ([PKCorso] ASC),
    CONSTRAINT [FK_Dim_Corso_PKDataInizioCorso] FOREIGN KEY ([PKDataInizioCorso]) REFERENCES [Dim].[Data] ([PKData])
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Corso_id_Corso]
    ON [Dim].[Corso]([IDCorso] ASC);


GO

