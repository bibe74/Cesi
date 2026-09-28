CREATE TABLE [Landing].[COMETA_Registro] (
    [id_registro]            INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [id_esercizio]           INT            NULL,
    [tipo_registro]          NVARCHAR (MAX) NULL,
    [id_mod_registro]        INT            NULL,
    [numero]                 NUMERIC (18)   NULL,
    [descrizione]            NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_Landing_COMETA_Registro] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_registro] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Registro_BusinessKey]
    ON [Landing].[COMETA_Registro]([id_registro] ASC);


GO

