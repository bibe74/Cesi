CREATE TABLE [Landing].[MYSOLUTION_Partecipant] (
    [Id]                     INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [FirstName]              NVARCHAR (MAX) NULL,
    [LastName]               NVARCHAR (MAX) NULL,
    [Email]                  NVARCHAR (MAX) NULL,
    [PhoneNumber]            NVARCHAR (MAX) NULL,
    [Ssn]                    NVARCHAR (MAX) NULL,
    [CreatedOnUtc]           DATETIME2 (7)  NOT NULL,
    [IdProfession]           INT            NOT NULL,
    [IdProfessionDetail]     INT            NOT NULL,
    [MobilePhone]            NVARCHAR (MAX) NULL,
    [OriginalPartecipantId]  INT            NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Partecipant] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Partecipant_BusinessKey]
    ON [Landing].[MYSOLUTION_Partecipant]([Id] ASC);


GO

