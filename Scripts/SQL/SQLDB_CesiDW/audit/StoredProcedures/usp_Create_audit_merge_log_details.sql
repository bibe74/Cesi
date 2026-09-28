
CREATE PROCEDURE audit.usp_Create_audit_merge_log_details
AS
BEGIN

SET NOCOUNT ON;

IF OBJECT_ID(N'audit.merge_log_details', N'U') IS NOT NULL DROP TABLE audit.merge_log_details;

CREATE TABLE audit.merge_log_details (
	merge_datetime				DATETIME CONSTRAINT DFT_audit_merge_log_details_merge_datetime DEFAULT(CURRENT_TIMESTAMP) NOT NULL,
	merge_action				NVARCHAR(10) NOT NULL,
	full_olap_table_name		NVARCHAR(261) NOT NULL,
	primary_key_description		NVARCHAR(1000) NOT NULL
);

CREATE NONCLUSTERED INDEX IX_audit_merge_log_details ON audit.merge_log_details (merge_datetime, merge_action, full_olap_table_name);

END;

GO

