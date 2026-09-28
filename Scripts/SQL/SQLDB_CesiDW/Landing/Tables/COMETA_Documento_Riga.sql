CREATE TABLE [Landing].[COMETA_Documento_Riga] (
    [id_riga_documento]         INT             NOT NULL,
    [HistoricalHashKey]         VARBINARY (20)  NULL,
    [ChangeHashKey]             VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]    VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]        VARCHAR (34)    NULL,
    [InsertDatetime]            DATETIME        NOT NULL,
    [UpdateDatetime]            DATETIME        NOT NULL,
    [IsDeleted]                 BIT             NULL,
    [id_documento]              INT             NULL,
    [id_gruppo_agenti]          INT             NULL,
    [num_riga]                  INT             NULL,
    [id_articolo]               INT             NULL,
    [descrizione]               NVARCHAR (MAX)  NULL,
    [totale_riga]               DECIMAL (10, 2) NULL,
    [provv_calcolata_carea]     DECIMAL (10, 2) NULL,
    [provv_calcolata_agente]    DECIMAL (10, 2) NULL,
    [provv_calcolata_subagente] DECIMAL (10, 2) NULL,
    [id_riga_doc_provenienza]   INT             NULL,
    CONSTRAINT [PK_Landing_COMETA_Documento_Riga] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_riga_documento] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Documento_Riga_BusinessKey]
    ON [Landing].[COMETA_Documento_Riga]([id_riga_documento] ASC);


GO

