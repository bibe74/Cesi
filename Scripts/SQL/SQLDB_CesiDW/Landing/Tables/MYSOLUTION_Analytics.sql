CREATE TABLE [Landing].[MYSOLUTION_Analytics] (
    [created_at]        DATE           NOT NULL,
    [email]             NVARCHAR (60)  NOT NULL,
    [path]              NVARCHAR (255) NOT NULL,
    [HistoricalHashKey] VARBINARY (32) NULL,
    [ChangeHashKey]     VARBINARY (32) NULL,
    [InsertDatetime]    DATETIME       NOT NULL,
    [UpdateDatetime]    DATETIME       NOT NULL,
    [IsDeleted]         BIT            NULL,
    [NumeroVisite]      INT            NULL,
    CONSTRAINT [PK_Landing_MYSOLUTION_Analytics] PRIMARY KEY CLUSTERED ([UpdateDatetime] ASC, [created_at] ASC, [email] ASC, [path] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Landing_MYSOLUTION_Analytics_BusinessKey]
    ON [Landing].[MYSOLUTION_Analytics]([created_at] ASC, [email] ASC, [path] ASC);


GO

