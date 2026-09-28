CREATE TABLE [Landing].[MYSOLUTION_GenericAttribute] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [EntityId]               INT            NOT NULL,
    [Key]                    NVARCHAR (400) NOT NULL,
    [Value]                  NVARCHAR (MAX) NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_GenericAttribute] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_GenericAttribute_BusinessKey]
    ON [Landing].[MYSOLUTION_GenericAttribute]([Id] ASC);


GO

