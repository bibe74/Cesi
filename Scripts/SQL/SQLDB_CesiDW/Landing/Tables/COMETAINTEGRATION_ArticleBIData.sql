CREATE TABLE [Landing].[COMETAINTEGRATION_ArticleBIData] (
    [ArticleID]              INT            NOT NULL,
    [HistoricalHashKey]      VARBINARY (20) NULL,
    [ChangeHashKey]          VARBINARY (20) NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)   NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)   NULL,
    [InsertDatetime]         DATETIME       NOT NULL,
    [UpdateDatetime]         DATETIME       NOT NULL,
    [IsDeleted]              BIT            NULL,
    [Data1]                  VARCHAR (2000) NULL,
    [Data2]                  VARCHAR (2000) NULL,
    [Data3]                  VARCHAR (2000) NULL,
    [Data4]                  VARCHAR (2000) NULL,
    [Data5]                  VARCHAR (2000) NULL,
    [Data6]                  VARCHAR (2000) NULL,
    CONSTRAINT [PK_Landing_COMETAINTEGRATION_ArticleBIData] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [ArticleID] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_COMETAINTEGRATION_ArticleBIData_BusinessKey]
    ON [Landing].[COMETAINTEGRATION_ArticleBIData]([ArticleID] ASC);


GO

