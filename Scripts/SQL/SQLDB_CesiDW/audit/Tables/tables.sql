CREATE TABLE [audit].[tables] (
    [provider_name]            NVARCHAR (60) NOT NULL,
    [full_table_name]          [sysname]     NOT NULL,
    [staging_table_name]       [sysname]     NOT NULL,
    [datawarehouse_table_name] [sysname]     NOT NULL,
    [lastupdated_staging]      DATETIME      NULL,
    [lastupdated_local]        DATETIME      NULL,
    CONSTRAINT [PK_audit_tables] PRIMARY KEY CLUSTERED ([provider_name] ASC, [full_table_name] ASC)
);


GO

