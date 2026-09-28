CREATE TABLE [Landing].[MYSOLUTION_CoursesData] (
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
    [CourseCode]             NVARCHAR (400)  NULL,
    [AttCourseCode]          NVARCHAR (400)  NULL,
    [WebinarCode]            NVARCHAR (400)  NULL,
    [AttWebinarCode]         NVARCHAR (400)  NULL,
    [CourseType]             NVARCHAR (MAX)  NULL,
    [StartDate_text]         NVARCHAR (400)  NULL,
    [StartDate]              DATE            NULL,
    [HasMoreDates]           BIT             NULL,
    [OrderDescription]       NVARCHAR (MAX)  NULL,
    [ItemNetUnitPrice]       NUMERIC (18, 4) NOT NULL,
    [OrderTotalPrice]        NUMERIC (18, 4) NOT NULL,
    [OrderNumber]            NVARCHAR (MAX)  NOT NULL,
    [OrderStatus]            INT             NOT NULL,
    [OrderCreatedDate]       DATE            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_CoursesData] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [OrderItemId] ASC, [Partecipant_Id] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_MYSOLUTION_CoursesData_BusinessKey]
    ON [Landing].[MYSOLUTION_CoursesData]([OrderItemId] ASC, [Partecipant_Id] ASC);


GO

