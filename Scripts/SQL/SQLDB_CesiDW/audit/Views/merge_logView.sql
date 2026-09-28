
CREATE VIEW audit.merge_logView
AS
SELECT
	merge_datetime,
	full_olap_table_name,
	COALESCE([1], 0) AS inserted_rows, COALESCE([2], 0) AS updated_rows, COALESCE([3], 0) AS deleted_rows
FROM (
	SELECT
		MLD.merge_datetime,
		MLD.full_olap_table_name,
		A.merge_action_id,
		1 AS recordCount

	FROM audit.merge_log_details MLD
	INNER JOIN (
		SELECT 1 AS merge_action_id, 'INSERT' AS merge_action
		UNION ALL SELECT 2, 'UPDATE'
		UNION ALL SELECT 3, 'DELETE'
	) A ON MLD.merge_action = A.merge_action
) AS SourceTable
PIVOT (
	COUNT(SourceTable.recordCount)
	FOR merge_action_id IN ([1], [2], [3])
) AS PivotTable
;

GO

