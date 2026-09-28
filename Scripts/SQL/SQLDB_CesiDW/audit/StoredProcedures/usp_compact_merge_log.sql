
CREATE PROCEDURE audit.usp_compact_merge_log
AS
BEGIN

SET NOCOUNT ON;

INSERT INTO audit.merge_log
SELECT * FROM audit.merge_logView
ORDER BY merge_datetime,
	full_olap_table_name;

TRUNCATE TABLE audit.merge_log_details;

END;

GO

