CREATE TABLE [Landing].[COMETA_CondizioniPagamento] (
    [id_con_pagamento]       INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Codice]                 NVARCHAR (MAX) NULL,
    [Descrizione]            NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_Landing_COMETA_CondizioniPagamento] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [id_con_pagamento] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETA_CondizioniPagamento_BusinessKey]
    ON [Landing].[COMETA_CondizioniPagamento]([id_con_pagamento] ASC);


GO

