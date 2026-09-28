CREATE TABLE [Staging].[AccessiUsername] (
    [UsernameAccessi] NVARCHAR (50) NOT NULL,
    [Username]        NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Staging_AccessiUsername] PRIMARY KEY CLUSTERED ([UsernameAccessi] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [IX_Staging_AccessiUsername_BusinessKey]
    ON [Staging].[AccessiUsername]([UsernameAccessi] ASC);


GO

