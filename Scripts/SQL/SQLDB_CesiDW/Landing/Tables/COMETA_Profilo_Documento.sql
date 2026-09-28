CREATE TABLE [Landing].[COMETA_Profilo_Documento] (
    [id_prof_documento]      INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (10)  NULL,
    [descrizione]            NVARCHAR (60)  NULL,
    [tipo_registro]          CHAR (2)       NULL,
    CONSTRAINT [PK_Landing_COMETA_Profilo_Documento] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_prof_documento] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Profilo_Documento_BusinessKey]
    ON [Landing].[COMETA_Profilo_Documento]([id_prof_documento] ASC);


GO

