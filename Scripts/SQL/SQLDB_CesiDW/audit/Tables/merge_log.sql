CREATE TABLE [audit].[merge_log] (
    [merge_datetime]       DATETIME       CONSTRAINT [DFT_audit_merge_log_merge_datetime] DEFAULT (getdate()) NOT NULL,
    [full_olap_table_name] NVARCHAR (261) NOT NULL,
    [inserted_rows]        INT            CONSTRAINT [DFT_audit_merge_log_inserted_rows] DEFAULT ((0)) NOT NULL,
    [updated_rows]         INT            CONSTRAINT [DFT_audit_merge_log_updated_rows] DEFAULT ((0)) NOT NULL,
    [deleted_rows]         INT            CONSTRAINT [DFT_audit_merge_log_deleted_rows] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_audit_merge_log] PRIMARY KEY CLUSTERED ([merge_datetime] ASC, [full_olap_table_name] ASC)
);


GO

