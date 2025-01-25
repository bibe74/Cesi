USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @table audit.merge_log_details
 * @description Origini dati e relativi schemi

*/

IF OBJECT_ID(N'audit.usp_Create_audit_merge_log_details', N'P') IS NOT NULL DROP PROCEDURE audit.usp_Create_audit_merge_log_details;
GO

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

IF OBJECT_ID(N'audit.merge_log_details', N'U') IS NULL EXEC audit.usp_Create_audit_merge_log_details;
GO

/**
 * @table audit.merge_log
 * @description Origini dati e relativi schemi

*/

IF OBJECT_ID(N'audit.merge_logView', N'V') IS NOT NULL DROP VIEW audit.merge_logView;
GO

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

IF OBJECT_ID(N'audit.usp_Create_audit_merge_log', N'P') IS NOT NULL DROP PROCEDURE audit.usp_Create_audit_merge_log;
GO

CREATE PROCEDURE audit.usp_Create_audit_merge_log
AS
BEGIN

SET NOCOUNT ON;

IF OBJECT_ID(N'audit.merge_log', N'U') IS NOT NULL DROP TABLE audit.merge_log;

CREATE TABLE audit.merge_log (
	merge_datetime				DATETIME CONSTRAINT DFT_audit_merge_log_merge_datetime DEFAULT(CURRENT_TIMESTAMP) NOT NULL,
	full_olap_table_name		NVARCHAR(261) NOT NULL,
	inserted_rows				INT CONSTRAINT DFT_audit_merge_log_inserted_rows DEFAULT(0) NOT NULL,
	updated_rows				INT CONSTRAINT DFT_audit_merge_log_updated_rows DEFAULT(0) NOT NULL,
	deleted_rows				INT CONSTRAINT DFT_audit_merge_log_deleted_rows DEFAULT(0) NOT NULL,

	CONSTRAINT PK_audit_merge_log PRIMARY KEY CLUSTERED (
		merge_datetime,
		full_olap_table_name
	)
);

END;
GO

IF OBJECT_ID(N'audit.merge_log', N'U') IS NULL EXEC audit.usp_Create_audit_merge_log;
GO

/**
 * @storedprocedure audit.usp_compact_merge_log
 * @description Script per consolidamento log delle operazione merge
*/

IF OBJECT_ID(N'audit.usp_compact_merge_log', N'P') IS NOT NULL DROP PROCEDURE audit.usp_compact_merge_log;
GO

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

EXEC audit.usp_compact_merge_log;
GO

/**
 * @table audit.tables
 * @description
*/

--DROP TABLE audit.tables;
GO

IF OBJECT_ID(N'audit.tables', N'U') IS NULL
BEGIN

    CREATE TABLE audit.tables (
        provider_name NVARCHAR(60) NOT NULL,
        full_table_name sysname NOT NULL,
        staging_table_name sysname NOT NULL,
        datawarehouse_table_name sysname NOT NULL,
        lastupdated_staging DATETIME NULL,
        lastupdated_local DATETIME NULL
    );

    ALTER TABLE audit.tables ADD CONSTRAINT PK_audit_tables PRIMARY KEY CLUSTERED (provider_name, full_table_name);

END;
GO

/**
 * @storedprocedure audit.usp_refresh_all_views
 * @description Aggiorna le views, mostrando eventuali errori

 * @source http://stackoverflow.com/questions/1177659/syntax-check-all-stored-procedures
*/

IF OBJECT_ID(N'audit.usp_refresh_all_views', N'P') IS NULL EXEC ('CREATE PROCEDURE audit.usp_refresh_all_views AS RETURN 0;');
GO

ALTER PROCEDURE audit.usp_refresh_all_views AS
BEGIN

	-- This sp will refresh all views in the catalog. 
	--     It enumerates all views, and runs sp_RefreshView for each of them

	DECLARE abc CURSOR FOR
	SELECT TABLE_SCHEMA + N'.' + TABLE_NAME AS ViewName
	FROM INFORMATION_SCHEMA.VIEWS
	ORDER BY TABLE_SCHEMA,
		TABLE_NAME;
	OPEN abc;

	DECLARE @ViewName varchar(261);

	-- Build select string
	DECLARE @SQLString nvarchar(2048);

	FETCH NEXT FROM abc INTO @ViewName;

	WHILE @@FETCH_STATUS = 0 
	BEGIN
		SET @SQLString = 'EXECUTE sp_RefreshView ''' + @ViewName + '''';
		--PRINT @SQLString

		BEGIN TRY
			EXECUTE sp_ExecuteSQL @SQLString;

			--PRINT 'OK ==> ' + @SQLString;
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0) ROLLBACK;

			PRINT 'KO ==> ' + @SQLString;
		END CATCH

		FETCH NEXT FROM abc INTO @ViewName;
	END;

	CLOSE abc;
	DEALLOCATE abc;

END;
GO

--EXEC audit.usp_refresh_all_views;
GO
