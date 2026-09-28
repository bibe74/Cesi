CREATE TABLE [Landing].[COMETA_Telefono] (
    [id_telefono]            INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [id_anagrafica]          INT            NULL,
    [tipo]                   CHAR (1)       NULL,
    [num_riferimento]        NVARCHAR (200) NULL,
    [descrizione]            NVARCHAR (400) NULL,
    [interlocutore]          NVARCHAR (MAX) NULL,
    [nome]                   NVARCHAR (MAX) NULL,
    [cognome]                NVARCHAR (MAX) NULL,
    [ruolo]                  NUMERIC (18)   NULL,
    CONSTRAINT [PK_Landing_COMETA_Telefono] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_telefono] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Telefono_BusinessKey]
    ON [Landing].[COMETA_Telefono]([id_telefono] ASC);


GO

