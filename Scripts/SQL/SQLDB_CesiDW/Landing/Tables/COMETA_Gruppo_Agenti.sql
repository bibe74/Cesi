CREATE TABLE [Landing].[COMETA_Gruppo_Agenti] (
    [id_gruppo_agenti]       INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [codice]                 NVARCHAR (10)  NULL,
    [descrizione]            NVARCHAR (60)  NULL,
    [id_sog_com_capo_area]   INT            NULL,
    [id_sog_com_agente]      INT            NULL,
    [id_sog_com_sub_agente]  INT            NULL,
    CONSTRAINT [PK_Landing_COMETA_Gruppo_Agenti] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_gruppo_agenti] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_Gruppo_Agenti_BusinessKey]
    ON [Landing].[COMETA_Gruppo_Agenti]([id_gruppo_agenti] ASC);


GO

