CREATE TABLE [Landing].[COMETA_Documento] (
    [id_documento]               INT             NOT NULL,
    [HistoricalHashKey]          VARBINARY (20)  NULL,
    [ChangeHashKey]              VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII]     VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]         VARCHAR (34)    NULL,
    [InsertDatetime]             DATETIME        NOT NULL,
    [UpdateDatetime]             DATETIME        NOT NULL,
    [IsDeleted]                  BIT             NULL,
    [id_prof_documento]          INT             NULL,
    [id_registro]                INT             NULL,
    [data_registrazione]         DATE            NULL,
    [num_documento]              NVARCHAR (20)   NULL,
    [data_documento]             DATE            NULL,
    [data_competenza]            DATE            NULL,
    [id_sog_commerciale]         INT             NULL,
    [id_sog_commerciale_fattura] INT             NULL,
    [id_gruppo_agenti]           INT             NULL,
    [data_fine_contratto]        DATE            NULL,
    [libero_4]                   NVARCHAR (200)  NULL,
    [data_inizio_contratto]      DATE            NULL,
    [id_libero_1]                INT             NULL,
    [id_libero_2]                INT             NULL,
    [id_libero_3]                INT             NULL,
    [id_tipo_fatturazione]       INT             NULL,
    [data_disdetta]              DATE            NULL,
    [motivo_disdetta]            NVARCHAR (120)  NULL,
    [id_con_pagamento]           INT             NULL,
    [rinnovo_automatico]         CHAR (1)        NULL,
    [note_intestazione]          NVARCHAR (1000) NULL,
    [note_decisionali]           NVARCHAR (1000) NULL,
    CONSTRAINT [PK_Landing_COMETA_Documento] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_documento] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Documento_BusinessKey]
    ON [Landing].[COMETA_Documento]([id_documento] ASC);


GO

