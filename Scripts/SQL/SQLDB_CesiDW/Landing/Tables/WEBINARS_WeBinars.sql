CREATE TABLE [Landing].[WEBINARS_WeBinars] (
    [Source]                 NVARCHAR (50)  NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [VideoStartDate]         DATETIME       NULL,
    [VideoTitle]             NVARCHAR (500) NULL,
    [CourseTitle]            NVARCHAR (500) NULL,
    [CourseType]             VARCHAR (500)  NULL,
    CONSTRAINT [PK_Landing_WEBINARS_WeBinars] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [Source] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_WEBINARS_WeBinars_BusinessKey]
    ON [Landing].[WEBINARS_WeBinars]([Source] ASC);


GO

