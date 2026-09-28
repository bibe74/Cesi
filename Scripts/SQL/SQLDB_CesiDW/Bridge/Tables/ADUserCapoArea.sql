CREATE TABLE [Bridge].[ADUserCapoArea] (
    [ADUser]   NVARCHAR (60) NOT NULL,
    [CapoArea] NVARCHAR (60) NOT NULL,
    CONSTRAINT [PK_Bridge_ADUserCapoArea] PRIMARY KEY CLUSTERED ([ADUser] ASC, [CapoArea] ASC)
);


GO

