CREATE TABLE [Landing].[MYSOLUTION_Courses] (
    [OrderItemId]            INT             NOT NULL,
    [Partecipant_Id]         INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NULL,
    [PartecipantFirstName]   NVARCHAR (MAX)  NULL,
    [PartecipantLastName]    NVARCHAR (MAX)  NULL,
    [PartecipantEmail]       NVARCHAR (MAX)  NULL,
    [PartecipantFiscalCode]  NVARCHAR (MAX)  NULL,
    [RootPartecipantEmail]   NVARCHAR (MAX)  NULL,
    [CustomerUserName]       NVARCHAR (1000) NULL,
    [CourseName]             NVARCHAR (400)  NOT NULL,
    [CourseType]             NVARCHAR (MAX)  NULL,
    [StartDate_text]         NVARCHAR (400)  NULL,
    [StartDate]              DATE            NULL,
    [OrderNumber]            NVARCHAR (MAX)  NOT NULL,
    [OrderCreatedDate]       DATETIME2 (7)   NOT NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Courses] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [OrderItemId] ASC, [Partecipant_Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_Courses_BusinessKey]
    ON [Landing].[MYSOLUTION_Courses]([OrderItemId] ASC, [Partecipant_Id] ASC);


GO

